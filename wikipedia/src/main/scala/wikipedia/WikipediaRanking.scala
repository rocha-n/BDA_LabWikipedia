package wikipedia

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class WikipediaArticle(title: String, text: String) {

  /**
    * Helper function to generate word list.
    *
    * Words are seperated by spaces and ponctuation.
    * Note: +#- punctuation characters are not used,
    * because they are part of some programming languages name.
    */
  def words: Array[String] = text
      .toLowerCase()
      .replaceAll("[!\"$%&'()*,./:;<=>?@\\[\\]^_`{|}~]", " ")
      .split(" ")

  /**
    * Helper function to check whether this mentions the programming language.
    */
  def containsLang(lang: String): Boolean = words.contains(lang.toLowerCase())
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /**
    * List((JavaScript,1769), (PHP,1416), (Java,891), (C#,785), (CSS,580), (Python,556), (C++,553), (MATLAB,343), (Perl,317), (Ruby,273), (Haskell,128), (Objective-C,111), (Scala,96), (Groovy,62), (Clojure,59))
    * List((JavaScript,1769), (PHP,1416), (Java,891), (C#,785), (CSS,580), (Python,556), (C++,553), (MATLAB,343), (Perl,317), (Ruby,273), (Haskell,128), (Objective-C,111), (Scala,96), (Groovy,62), (Clojure,59))
    * List((JavaScript,1769), (PHP,1416), (Java,891), (C#,785), (CSS,580), (Python,556), (C++,553), (MATLAB,343), (Perl,317), (Ruby,273), (Haskell,128), (Objective-C,111), (Scala,96), (Groovy,62), (Clojure,59))
    *
    * Processing Part 1: naive ranking took 45991 ms.
    * Processing Part 2: ranking using inverted index took 25603 ms.
    * Processing Part 3: ranking using reduceByKey took 22242 ms.
    */

  val conf: SparkConf = new SparkConf().setAppName("wikipediaArticle").setMaster(s"local[*]")

  val sc: SparkContext = new SparkContext(conf)
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.articles)

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: should you count the "Java" language when you see "JavaScript"?
    * Hint3: the only whitespaces are blanks " "
    * Hint4: no need to search in the title :)
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.aggregate(0)((acc, wi) => if (wi.containsLang(lang)) acc+1 else acc, _ + _)
  }

  /** (1) Use `occurrencesOfLang` to compute the ranking of the languages
    * (`langs`) by determining the number of Wikipedia articles that
    * mention each language at least once. Don't forget to sort the
    * languages by their occurrence, in decreasing order!
    *
    * Note: this operation is long-running. It can potentially run for
    * several seconds.
    */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortWith(_._2 > _._2)
  }

  /** Compute an inverted index of the set of articles, mapping each language
    * to the Wikipedia pages in which it occurs.
    */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd.flatMap(wi => langs.filter(lang => wi.containsLang(lang)).map(lang => (lang, wi))).groupByKey()
  }

  /** (2) Compute the language ranking again, but now using the inverted index. Can you notice
    * a performance improvement?
    *
    * Note: this operation is long-running. It can potentially run for
    * several seconds.
    */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.map(t => (t._1, t._2.size)).collect().sortWith(_._2 > _._2).toList
  }

  /** (3) Use `reduceByKey` so that the computation of the index and the ranking is combined.
    * Can you notice an improvement in performance compared to measuring *both* the computation of the index
    * and the computation of the ranking? If so, can you think of a reason?
    *
    * Note: this operation is long-running. It can potentially run for
    * several seconds.
    */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap(wi => langs.filter(lang => wi.containsLang(lang)).map(t => (t, 1)))
      .reduceByKey(_+_).collect().sortWith(_._2 > _._2).toList
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    println(langsRanked)
    println(langsRanked2)
    println(langsRanked3)

    /* Output the speed of each ranking */
    println(timing)
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}

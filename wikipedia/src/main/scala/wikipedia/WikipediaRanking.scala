package wikipedia

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /**
    * DMA: Temps avec un seul thread.
    * Processing Part 1: naive ranking took 44609 ms.
    * Processing Part 2: ranking using inverted index took 18156 ms.
    * Processing Part 3: ranking using reduceByKey took 13907 ms.
    *
    * DMA: Temps avec le nombre de process de la machine.
    * Processing Part 1: naive ranking took 25914 ms.
    * Processing Part 2: ranking using inverted index took 9359 ms.
    * Processing Part 3: ranking using reduceByKey took 6641 ms.
    *
    * List((JavaScript,1692), (C#,706), (Java,586), (CSS,372), (C++,334), (MATLAB,295), (Python,286), (PHP,279), (Perl,144), (Ruby,120), (Haskell,54), (Objective-C,47), (Scala,43), (Clojure,26), (Groovy,23))
    * List((JavaScript,1692), (C#,706), (Java,586), (CSS,372), (C++,334), (MATLAB,295), (Python,286), (PHP,279), (Perl,144), (Ruby,120), (Haskell,54), (Objective-C,47), (Scala,43), (Clojure,26), (Groovy,23))
    * List((JavaScript,1692), (C#,706), (Java,586), (CSS,372), (C++,334), (MATLAB,295), (Python,286), (PHP,279), (Perl,144), (Ruby,120), (Haskell,54), (Objective-C,47), (Scala,43), (Clojure,26), (Groovy,23))
    *
    * --------------------------------Avec lowerCase-----------------------------
    * DMA: Temps avec un seul thread.
    * Processing Part 1: naive ranking took 64750 ms.
    * Processing Part 2: ranking using inverted index took 41297 ms.
    * Processing Part 3: ranking using reduceByKey took 37703 ms.
    *
    * DMA: Temps avec le nombre de process de la machine.
    * Processing Part 1: naive ranking took 35735 ms.
    * Processing Part 2: ranking using inverted index took 18312 ms.
    * Processing Part 3: ranking using reduceByKey took 15782 ms.
    *
    * List((JavaScript,1721), (C#,707), (Java,618), (CSS,400), (C++,335), (Python,315), (MATLAB,307), (PHP,302), (Perl,167), (Ruby,125), (Haskell,56), (Objective-C,47), (Scala,44), (Clojure,26), (Groovy,26))
    * List((JavaScript,1721), (C#,707), (Java,618), (CSS,400), (C++,335), (Python,315), (MATLAB,307), (PHP,302), (Perl,167), (Ruby,125), (Haskell,56), (Objective-C,47), (Scala,44), (Clojure,26), (Groovy,26))
    * List((JavaScript,1721), (C#,707), (Java,618), (CSS,400), (C++,335), (Python,315), (MATLAB,307), (PHP,302), (Perl,167), (Ruby,125), (Haskell,56), (Objective-C,47), (Scala,44), (Clojure,26), (Groovy,26))
    */

  /**
    * DRO: Temps avec un seul thread.
    * Processing Part 1: naive ranking took 54571 ms.
    * Processing Part 2: ranking using inverted index took 26538 ms.
    * Processing Part 3: ranking using reduceByKey took 23593 ms.
    *
    * DRO: Temps avec le nombre de process de la machine.
    * Processing Part 1: naive ranking took 32798 ms.
    * Processing Part 2: ranking using inverted index took 13250 ms.
    * Processing Part 3: ranking using reduceByKey took 10607 ms.
    */

//  val conf: SparkConf = new SparkConf().setAppName("wikipediaArticle").setMaster(s"local[${Runtime.getRuntime.availableProcessors()}]")
  val conf: SparkConf = new SparkConf().setAppName("wikipediaArticle").setMaster(s"local[1]")

  val sc: SparkContext = new SparkContext(conf)
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.articles)

  /**
    * Helper function to check whether an article mentions the programming language.
    */
  def containsLang(lang: String, wi: WikipediaArticle): Boolean = {
    wi.text.toLowerCase().split(" ").contains(lang.toLowerCase())
  }

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: should you count the "Java" language when you see "JavaScript"?
    * Hint3: the only whitespaces are blanks " "
    * Hint4: no need to search in the title :)
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.aggregate(0)((acc, wi) => if (containsLang(lang, wi)) acc+1 else acc, _ + _)
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
    rdd.flatMap(wi => langs.filter(lang => containsLang(lang, wi)).map(lang => (lang, wi))).groupByKey()
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
    rdd.flatMap(wi => langs.filter(l => containsLang(l, wi)).map(t => (t, 1)))
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

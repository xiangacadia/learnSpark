val lines = sc.textFile("/home/ubuntu/spark/README.md")
sc
lines.count()
lines.first()

val testFile = sc.textFile("/home/ubuntu/learnSpark/testFile")
val foc = sc.textFile("/home/ubuntu/learnSpark/testProfile").map(
line => {
    val temp = line.split(" ")
    ((temp(0).toLong, math.log(5.0 / (temp(1).toDouble + 1.0) + 1.0)))
}
)

testFile.count()
testFile.first()
testFile.take(2)

testFile.map(_.toUpperCase)


// split from text into key-value pairs
val fa = testFile.map(line => {
    val temp = line.split(" ")
    (temp(0).toLong, temp(1).toLong)
})

// total number of unique users
val total = fa.map( _._2 ).distinct.count

// filter users with less than 2 followers
val filtered = fa.map( v => (v._1, 1) ).reduceByKey(_+_).filter( _._2 > 1)

// joint to a vector
val vectors = fa.join(filtered).join( foc )

val normVectors = vectors.map( v => (v._2._1._1, (v._1, v._2._2) ) ).join( magsq ).map( v => (v._2._1._1, (v._1, v._2._1._2 / math.sqrt(v._2._2 ) ) ) )


val magsq = vectors.map( v => (v._2._1._1, v._2._2 * v._2._2) ).reduceByKey(_+_)

 val normVectors = vectors.map( v => (v._2._1._1, (v._1, v._2._2) ) ).join( magsq ).map( v => (v._2._1._1, (v._1, v._2._1._2 / math.sqrt(v._2._2 ) ) ) )

val k = 2
val startIds = normVectors.map( _._2._1 ).takeSample(false, k, 3)

var centroids = normVectors.filter( v => startIds.contains( v._2._1 ) ).map( v => (v._1, (startIds.indexWhere(_ == v._2._1), v._2._2) ) )

val bestMatches = normVectors.join( centroids ).map( v => ( ( v._2._1._1, v._2._2._1 ), v._2._1._2 * v._2._2._2 ) ).reduceByKey(_+_).map( v => (v._1._1, (v._1._2, v._2) ) ).reduceByKey( (a,b) => {
if(a._2 > b._2)
a
else
b
}).map( v => (v._1, v._2._1) )

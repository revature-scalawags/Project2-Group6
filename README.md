# Twitter Analysis - Group 6

### Spark AWS Application that inputs stream of twitter data and provides analysis based on our findings
 
## Questions
1.What information can be found regarding trending tweets based on the number of likes?

2.Which url is the most referenced in a given stream?

3.Can we find out which tweets contain negative language?

## Technologies 
1. Amazon Web Service 
2. sbt 
3. Scala 2.12.13
4. Apache Spark 3.0.0  
5. YARN 1.0
6. Hadoop File system 2.10

## Features
1. TrendingNegativity - Given a list of keywords, outputs a list counting the use of negative words in a stream

2. TrendingTweets - Given a stream, outputs a dataset of the most trending tweets based on the number of likes

3. MostMentionedUrl - Given a stream, outputs the url with the most mentions

4. FollowerRecommender – Given a user, outputs their recommended followers 

###  By David Masterson, Page Tyler, Zeshawn Manzoor and Nick Rau 

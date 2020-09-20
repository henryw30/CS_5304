# Wordcount Program

## Running code:

Assuming apache-spark and python3 are already installed run the following:

```
spark-submit wordcount.py wiki.txt [output directory]
```

## Outputs:

1. Original word count output is in wordcount/part-00000
2. List of bigrams is in bigrams/part-00000
3. Conditional bigram output is in cond_bigrams/part-00000

## Interpreting Conditional Bigrams:

First tuple element is bigram, second tuple element is probability of the 2nd word appearing after the first </p>

* e.g. ('san francisco', 0.3173605655930872) means 'francisco' has a 31.7% chance of occurring after 'san'
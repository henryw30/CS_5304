import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)

def splitLine(line):
    split_words = line.split(" ")
    output = []
    for i in range(1, len(split_words)):
        output.append((split_words[i - 1], split_words[i]))
    return output

def emit(word):
    return (word[0], (word[0], word[1]))


def combine(tu):
    bigrams = list(tu[1])
    word_dict = {}
    ret_list = []

    for i in bigrams:
        temp = str(i[0]) + " " + str(i[1])
        if (temp in word_dict):
            word_dict[temp] += 1
        else:
            word_dict[temp] = 1

    num_first = len(bigrams)

    for pair, freq in word_dict.items():
        ret_list.append((pair, freq / num_first))

    return ret_list


words = sc.textFile(sys.argv[1]).flatMap(splitLine)

wordCounts = words.map(emit).groupByKey().flatMap(combine)

wordCounts.coalesce(1, shuffle=True).saveAsTextFile(sys.argv[2])
sc.stop()
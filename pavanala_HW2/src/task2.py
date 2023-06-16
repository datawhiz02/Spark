def task2(sc, files):
    import re, nltk
        
    from nltk.corpus import stopwords
    
    stopwords = stopwords.words('english')
    
    output = None
    
    counts_list = []

    
    count = [sc.textFile(file) \
        .flatMap(lambda x: x.split(' ')) \
        .map(lambda x: re.sub(r'[^\w\s]', '', x)) \
        .map(lambda x: (x.lower(),1)) \
        .filter(lambda x: x[0] not in stopwords + ['']) \
        .reduceByKey(lambda a,b: a+b) for file in files]
        
        
    counts = sc.union(count) \
        .sortBy(lambda pair: pair[1], ascending=False)
        
#     counts.saveAsTextFile("C:/DIC-Project/HW2/output")
    return counts

if __name__ == '__main__':
    from pyspark.context import SparkContext
    sc = SparkContext('local', 'test')
    
    results = task2(sc, ['book1.txt', 'book2.txt', 'book3.txt', 'book4.txt', 'book5.txt','book6.txt', 'book7.txt', 'book8.txt', 'book9.txt', 'book10.txt'])

    results.saveAsTextFile("C:/DIC-Project/HW2/output2")
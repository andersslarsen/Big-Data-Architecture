from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, lower, regexp_replace, udf
from pyspark.sql.types import ArrayType, StringType
import sys
import base64
import string
import re
from graphframes import *
from itertools import permutations

conf = SparkConf().setAppName("project").setMaster("local[*]")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
sc.setLogLevel("ERROR")

INPUT_DATA_PATH = sys.argv[1]
INPUT_POST_ID = sys.argv[2]

def removeJunk(termList):
    new_set = [x.replace('pxaxap', '').replace('pxa', '').replace('bp', '').replace('.','') for x in termList]
    #new_set = [x.replace('.','') for x in termList]
    return new_set

def getUniqueWords(tokensList):
    unique = []
    for item in tokensList:
        if item not in unique:
            unique.append(item)
    return unique

def removePuncSym(someString):
    punctuations = '''!()-[]{};:'"\,<>/?@#$%^&*_~'''
    noPuncSym = ""
    for char in someString:
        if char not in punctuations:
            noPuncSym = noPuncSym + char
    return noPuncSym

def removeStopWords(someList, swList):
    filteredSentence = [w for w in someList if not w in swList]
    filteredSentence = []
    for w in someList:
        if w not in swList:
            filteredSentence.append(w)
    return filteredSentence

def removeShortWords(someList):
    filterLength = [w for w in someList if len(w)>=3]
    filterLength = []
    for w in someList:
        if len(w)>=3:
            filterLength.append(w)
    return filterLength

def windowSlider(someList):
    W = []
    S = []
    for w in someList:
        if len(W)==5:
            edges = permutations(W,2)
            for perm in edges:
                if perm[0]!=perm[1]:
                    S.append(perm)
            W.pop(0)
        W.append(w)
    return S



stopwordList = ["a","about","above","after","again","against","ain","all","am",
"an","and","any","are","aren","aren't","as","at","be","because","been","before","being",
"below","between","both","but","by","can","couldn","couldn't","d","did","didn",
"didn't","do","does","doesn","doesn't","doing","don","don't","down","during",
"each","few","for","from","further","had","hadn","hadn't","has","hasn","hasn't",
"have","haven","haven't","having","he","her","here","hers","herself","him",
"himself","his","how","i","if","in","into","is","isn","isn't","it","it's","its",
"itself","just","ll","m","ma","me","mightn","mightn't","more","most","mustn",
"mustn't","my","myself","needn","needn't","no","nor","not","now","o","of","off",
"on","once","only","or","other","our","ours","ourselves","out","over","own","re"
,"s","same","shan","shan't","she","she's","should","should've","shouldn",
"shouldn't","so","some","such","t","than","that","that'll","the","their",
"theirs","them","themselves","then","there","these","they","this","those",
"through","to","too","under","until","up","ve","very","was","wasn","wasn't","we"
,"were","weren","weren't","what","when","where","which","while","who","whom",
"why","will","with","won","won't","wouldn","wouldn't","y","you","you'd","you'll"
,"you're","you've","your","yours","yourself","yourselves","could","he'd","he'll"
,"he's","here's","how's","i'd","i'll","i'm","i've","let's","ought","she'd",
"she'll","that's","there's","they'd","they'll","they're","they've","we'd",
"we'll","we're","we've","what's","when's","where's","who's","why's","would",
"able","abst","accordance","according","accordingly","across","act","actually",
"added","adj","affected","affecting","affects","afterwards","ah","almost",
"alone","along","already","also","although","always","among","amongst",
"announce","another","anybody","anyhow","anymore","anyone","anything","anyway",
"anyways","anywhere","apparently","approximately","arent","arise","around",
"aside","ask","asking","auth","available","away","awfully","b","back","became",
"become","becomes","becoming","beforehand","begin","beginning","beginnings",
"begins","behind","believe","beside","besides","beyond","biol","brief","briefly"
,"c","ca","came","cannot","can't","cause","causes","certain","certainly","co",
"com","come","comes","contain","containing","contains","couldnt","date",
"different","done","downwards","due","e","ed","edu","effect","eg","eight",
"eighty","either","else","elsewhere","end","ending","enough","especially","et",
"etc","even","ever","every","everybody","everyone","everything","everywhere",
"ex","except","f","far","ff","fifth","first","five","fix","followed","following"
,"follows","former","formerly","forth","found","four","furthermore","g","gave",
"get","gets","getting","give","given","gives","giving","go","goes","gone","got",
"gotten","h","happens","hardly","hed","hence","hereafter","hereby","herein",
"heres","hereupon","hes","hi","hid","hither","home","howbeit","however",
"hundred","id","ie","im","immediate","immediately","importance","important",
"inc","indeed","index","information","instead","invention","inward","itd",
"it'll","j","k","keep","keeps","kept","kg","km","know","known","knows","l",
"largely","last","lately","later","latter","latterly","least","less","lest",
"let","lets","like","liked","likely","line","little","'ll","look","looking",
"looks","ltd","made","mainly","make","makes","many","may","maybe","mean","means"
,"meantime","meanwhile","merely","mg","might","million","miss","ml","moreover",
"mostly","mr","mrs","much","mug","must","n","na","name","namely","nay","nd",
"near","nearly","necessarily","necessary","need","needs","neither","never",
"nevertheless","new","next","nine","ninety","nobody","non","none","nonetheless",
"noone","normally","nos","noted","nothing","nowhere","obtain","obtained",
"obviously","often","oh","ok","okay","old","omitted","one","ones","onto","ord",
"others","otherwise","outside","overall","owing","p","page","pages","part",
"particular","particularly","past","per","perhaps","placed","please","plus",
"poorly","possible","possibly","potentially","pp","predominantly","present",
"previously","primarily","probably","promptly","proud","provides","put","q",
"que","quickly","quite","qv","r","ran","rather","rd","readily","really","recent"
,"recently","ref","refs","regarding","regardless","regards","related",
"relatively","research","respectively","resulted","resulting","results","right",
"run","said","saw","say","saying","says","sec","section","see","seeing","seem",
"seemed","seeming","seems","seen","self","selves","sent","seven","several",
"shall","shed","shes","show","showed","shown","showns","shows","significant",
"significantly","similar","similarly","since","six","slightly","somebody",
"somehow","someone","somethan","something","sometime","sometimes","somewhat",
"somewhere","soon","sorry","specifically","specified","specify","specifying",
"still","stop","strongly","sub","substantially","successfully","sufficiently",
"suggest","sup","sure","take","taken","taking","tell","tends","th","thank",
"thanks","thanx","thats","that've","thence","thereafter","thereby","thered",
"therefore","therein","there'll","thereof","therere","theres","thereto",
"thereupon","there've","theyd","theyre","think","thou","though","thoughh",
"thousand","throug","throughout","thru","thus","til","tip","together","took",
"toward","towards","tried","tries","truly","try","trying","ts","twice","two","u"
,"un","unfortunately","unless","unlike","unlikely","unto","upon","ups","us",
"use","used","useful","usefully","usefulness","uses","using","usually","v",
"value","various","'ve","via","viz","vol","vols","vs","w","want","wants","wasnt"
,"way","wed","welcome","went","werent","whatever","what'll","whats","whence",
"whenever","whereafter","whereas","whereby","wherein","wheres","whereupon",
"wherever","whether","whim","whither","whod","whoever","whole","who'll",
"whomever","whos","whose","widely","willing","wish","within","without","wont",
"words","world","wouldnt","www","x","yes","yet","youd","youre","z","zero","a's",
"ain't","allow","allows","apart","appear","appreciate","appropriate",
"associated","best","better","c'mon","c's","cant","changes","clearly",
"concerning","consequently","consider","considering","corresponding","course",
"currently","definitely","described","despite","entirely","exactly","example",
"going","greetings","hello","help","hopefully","ignored","inasmuch","indicate",
"indicated","indicates","inner","insofar","it'd","keep","keeps","novel",
"presumably","reasonably","second","secondly","sensible","serious","seriously",
"sure","t's","third","thorough","thoroughly","three","well","wonder"]

postRDD = sc.textFile(INPUT_DATA_PATH + "/posts.csv.gz")
postHeader = postRDD.first()
postRDD = postRDD.filter(lambda x : x != postHeader) \
            .map(lambda lines : lines.split("\t")) \
            .filter(lambda x : x[0]==INPUT_POST_ID)

#Decoding content of posts and turn all characters to lower case
decodeRDD = postRDD.map(lambda x : (x[0], base64.b64decode(x[5]).lower()))

#Remove all punctuations and symbols, except "DOT"
remove_punc = decodeRDD.map(lambda line : (line[0], removePuncSym(str(line[1]))))

#Tokenize text
tokenize = remove_punc.map(lambda line : (line[0], re.split('\s+', line[1])))

#Remove stopwords
removeStopW = tokenize.map(lambda line : (line[0], removeStopWords(line[1], stopwordList)))

#Remove "DOT" and other junk
clean = removeStopW.map(lambda line : (line[0], removeJunk(line[1])))

#Remove tokens smaller than len(char)=3
removeShortW = clean.map(lambda line : (line[0], removeShortWords(line[1])))


#####CREATE EDGES AND VERTICES FOR THE GRAPH#####
##Edges: if two terms are in the same window, they have an edge inbetween them

edgesRDD = removeShortW.filter(lambda x : x[0]==INPUT_POST_ID) \
                .map(lambda line : windowSlider(line[1])) \
                .collect()
print(edgesRDD)

# edgesRDD = removeShortW.map(lambda line : (line[0], windowSlider(line[1]))) \
#                 .map(lambda x : (x[0], x[1][0][0], x[1][0][1])).take(5)

# edges = spark.createDataFrame(edgesRDD, ['PostID', 'Src', 'Dst'] )
# edges.printSchema()
# edges.show()

##Vertices: Each unique term from the sequence of terms is a node
#List of distinct words

# verticesRDD = removeShortW.map(lambda line : (line[0],getUniqueWords(line[1])))
#
# vertices = spark.createDataFrame(verticesRDD, ['PostID', 'Words'])
# vertices.printSchema()
# vertices.show(5)

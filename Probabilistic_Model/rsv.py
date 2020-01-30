# coding=utf-8
import json
import os
import sys, getopt
import unicodedata
import math
import collections
import operator

def totalDoc(file_index):
    #calculate total # of docuemnts in the corpus (N)
    os.system("galago dump-index-manifest ./corpus" + " > numDoc")
    num_of_doc = -1
    fp = open("numDoc", "r")
    flag = 0
    while fp:
        line_number = 0
        for line in fp: 
            var = "keyCount"
            if var in line:
                index = line.find(":") 
                num_of_doc = int(line[index+2:len(line)-2])
                flag = 1
                break
            line_number+=1
        if flag == 1:
            break
    return num_of_doc

def parseJudgement(file_index,jugdment_file):
    #calculate # of dcuments containing th query(n)
    queryfile = open(jugdment_file, "r")
    all_quries = json.load(queryfile)
    value = ""
    query_dictionary = {}
    classname = ""
    for x in all_quries:
        classname = x

    for q in all_quries[classname]:
        value = unicodedata.normalize('NFKD', q['text']).encode('ascii','ignore')
        start = value.find("(") + 1
        end = value.find(")")
        value = value[start: end]
        index = int(unicodedata.normalize('NFKD', q['number']).encode('ascii','ignore'))
        query_dictionary[index] = value
    return query_dictionary

def queryRSV(number_of_total_docs, query_dictionary, file_index):
    rsv_score_dictionary = {}
    # sorted_rsv_score_dictionary = {}
    for key, value in query_dictionary.items():
        rsv = 0
        word_list = set(value.split())
        for word in word_list:
            cmd = 'galago batch-search --index=' + file_index + ' --query=' + word + ' --requested=3024'
            os.system(cmd + '> docs')
            fp = open("docs","r")
            ni = len(fp.readlines())
            if ni == 0:
                rsv = 0
            else:
                rsv = math.log(number_of_total_docs / float(ni))
            frp = open("docs", "r")
            while frp:
                for line in frp:
                    doc_id = line.split()[2]
                    if doc_id not in rsv_score_dictionary:
                        rsv_score_dictionary[doc_id] = [key]
                        rsv_score_dictionary[doc_id].append(rsv)
                    else:
                        rsv_score_dictionary[doc_id][1] += rsv
                break
    sorted_rsv_score_dictionary = sorted(rsv_score_dictionary.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_rsv_score_dictionary
def ouputBase(rsv_score_dictionary):
    rank = 1
    f = open("base.out", "w")
    for key, value in rsv_score_dictionary:
        f.write(str(value[0]))
        f.write(" Q0 ")
        f.write(key + " ")
        f.write(str(rank) +" ")
        f.write(str(value[1]) + " ")
        f.write(" galago\n")
        rank += 1
#get list of relevant documents of the term (n) and list of document of query(S)
def getRelevantTerm(relfiles, query_dictionary,file_index):
    relevant_document_term_dict = dict()
    for key, value in query_dictionary.items():
        word_list = set(value.split())
        for word in word_list:
            cmd = "galago batch-search --index=" + file_index + " --query=" + word + " --requested=3024"
            os.system(cmd + '> docs')
            fp = open("docs","r")
            while fp:
                for line in fp:
                    doc_id = line.split()[2]
                    term_id = line.split()[3]
                    if word not in relevant_document_term_dict:
                        relevant_document_term_dict[word] = [key]
                        relevant_document_term_dict[word].append(doc_id)
                    else:
                        relevant_document_term_dict[word].append(doc_id)
                break
        #needs delete break for processing the whole dic
        #break
    return relevant_document_term_dict


def getRelevantQuery(relfiles):
    relevant_document_query_dict = dict()
    fp = open(relfiles,"r")
    while fp:
        for line in fp:
            query_id = int(line.split()[0])
            doc_id = line.split()[2]
            if query_id not in relevant_document_query_dict:
                relevant_document_query_dict[query_id] = [doc_id]
            else:
                relevant_document_query_dict[query_id].append(doc_id)
        break
    return relevant_document_query_dict

def getBIMScore(s, S, n, N):
    # down_1 = S - s 
    # down_2 = (N-n-S+s)


    # if (down_1 == 0):
    #     numerator = 0
    # else:
    #     numerator = (s/(S-s))

    # if (down_2 == 0):
    #     denominator = 0
    # else:
    #     denominator = ((n-s)/(N-n-S+s))

    # if numerator == 0:
    #     bim_score = 0
    # elif denominator == 0:
    #     bim_score = 0
    # else:
    #     print(numerator, denominator)
    #     numerator = (s / down_1)
    #     denominator = ((n-s) / down_2)
    #     innerNumber = abs(numerator / down)
    #     bim_score = math.log(innerNumber)

    bim_score = 0
    numerator = 0
    denominator = 0
    if s == S:
        numerator = 0
        # bim_score = log(s / float((n-s) / float(N-n-S+s)))
    else:
        numerator = float(s/(S-s)) 

    if (N-n-S+s) == 0:
        denominator = 0
        # bim_score = log(float(s / float(S-s)))
    else:
        denominator = float(n-s)/float(N-n-S+s)

    if numerator == 0:
        bim_score = 0
    elif denominator == 0:
        bim_score = 0
    else:
        numerator = ( float(s) / (S-s))
        denominator = (float(n-s) / (N-n-S+s))
        innerNumber = abs(numerator / denominator)
        bim_score = math.log(innerNumber)


        # numerator = float(s / float(S-s))
        # denominator = float((n-s) / float(N-n-S+s))
        # bim_score = math.log(numerator / denominator)
    
    return bim_score

def queryBIM(relevant_document_term, relevant_document_query, number_of_total_docs):
    intersect = []
    doc_score_dict = {}
    for key, value in relevant_document_term.items():
        if int(value[0]) in relevant_document_query:
            query_id = int(value[0])
            list1 = relevant_document_query[query_id]
            list2 = relevant_document_term[key]
            del list2[0]
            intersect =  list(set(list1) & set(list2)) 
            print intersect
            s = len(intersect)
            S = len(list1)
            n = len(list2)
            N = number_of_total_docs
            for doc_id in list2:
                if doc_id not in doc_score_dict:
                    doc_score_dict[doc_id] = [query_id]
                    doc_score_dict[doc_id].append(getBIMScore(s, S, n, N))
                    print doc_score_dict
                else:
                    doc_score_dict[doc_id][1] +=getBIMScore(s, S, n, N)
    sorted_doc_score_dict = sorted(doc_score_dict.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_doc_score_dict

def outputEstimate(doc_score_dict):
    rank = 1
    f = open("estimate.out", "w")
    for key, value in doc_score_dict:
        f.write(str(value[0]))
        f.write(" Q0 ")
        f.write(key + " ")
        f.write(str(rank) +" ")
        f.write(str(value[1]) + " ")
        f.write(" galago\n")
        rank += 1

if __name__ == "__main__":
    all_args = sys.argv
    jsonFile = all_args[1]
    relfiles = all_args[2]
    file_index = all_args[3]
    number_of_total_docs = totalDoc(file_index)
    query_dictionary = parseJudgement(file_index,jsonFile)
    sorted_rsv_score_dictionary = queryRSV(number_of_total_docs,query_dictionary, file_index)
    baseFile = ouputBase(sorted_rsv_score_dictionary)
    print ("end base")
    '''
    convertolist = "galago batch-search --index=./ " + "/homes/cs473/project1/" + jsonFile + " --requested=3024 > convert_baseline_list"
    os.system(convertolist)
    eval_func = "galago eval --judgments=/homes/cs473/project1/" + relfiles + " --baseline=convert_baseline_list > evalFile --details=true"
    os.system(eval_func)
    fp = open("evalFile")
    bim_query_dictionary = bim_query_list(fp)
    sorted_bim_score_dictionary = cal_bim_score(number_of_total_docs,bim_query_dictionary)
    '''
    
    # call batch search & eval for each term in the query
    relevant_document_term = getRelevantTerm(relfiles, query_dictionary, file_index)
    print("1")
    relevant_document_query = getRelevantQuery(relfiles)
    print("2")
    #get s by intersect two list
    doc_score_dict = queryBIM(relevant_document_term, relevant_document_query, number_of_total_docs)
    print("3")
    #write to file
    print("start estimate")
    estimateFile = outputEstimate(doc_score_dict)





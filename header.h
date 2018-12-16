#include <mpi.h>
#include <cstdio>
#include <cstdlib>
#include <bits/stdc++.h>
#include <string>
#include <dirent.h>
#include <fstream>
using namespace std;

struct docFreq
{
	int docID;
	int frequency;
};

struct serializedMap
{
	vector<char> charsInWords;
	vector<int> indexOfStrings;
	vector<int> docIDArr;
	vector<int> FreqArr;
	vector<int> indexDocFreq;
};

typedef docFreq docFreq;
typedef serializedMap serializedMap;
map<string,vector<docFreq> > createLocalIndex(char* dirPath, map<string, bool> stopWordsIndex,int my_rank);
bool compareDocFreq(docFreq &a,docFreq &b);
void printLocalIndex(map<string,vector<docFreq> > localIndex);
serializedMap serializeLocalMap(map<string, vector<docFreq> > localIndex);
map<string, pair<int,int> > deserializeMap(serializedMap s_map_recv);
map<string,vector<docFreq> > mergeHashMaps(map<string,vector<docFreq> > localIndex,map<string, pair<int,int> > de_map_recv,vector<int> docIDArr,vector<int> FreqArr,int my_rank,int level,int num_processes);
vector<docFreq> mergeVectors(vector<docFreq> vec1, vector<docFreq> vec2);
map<string,vector<docFreq> > deserializeAndMerge(map<string,vector<docFreq> > map1,serializedMap s_map);
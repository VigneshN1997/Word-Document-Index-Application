#include "header.h"

bool compareDocFreq(docFreq &a,docFreq &b)
{
	return a.frequency > b.frequency;
}


map<string,vector<docFreq> > createLocalIndex(char* dirPath, map<string, bool> stopWordsIndex,int my_rank)
{
	map<string,vector<docFreq> > localIndex;
	DIR* dir;
	struct dirent *docInfo;
	fstream doc;
	// unsigned int fileID = my_rank*1000000; 
	if((dir = opendir(dirPath)) != NULL)
	{
		while((docInfo = readdir(dir)) != NULL)
		{
			if(strcmp(docInfo->d_name,".") == 0 || strcmp(docInfo->d_name,"..") == 0)
			{
				continue;
			}
			unsigned int docID = atoi(docInfo->d_name); // file should not have an extension
			// unsigned int docID = fileID++;
			char* file_path = (char*)malloc(500*sizeof(char));
			strcpy(file_path,dirPath);
			strcat(file_path,"/");
			strcat(file_path,docInfo->d_name);
			doc.open(file_path);


			map<string,bool> isAlreadyPresent; // if word has occured at least once in the document opened
			string word;
			char temp_word[512];
			while(doc >> temp_word)
			{
				char* token = strtok(temp_word,"\n ,-.:;?!\"");
				while(token != NULL)
				{
					word = string(token);
					transform(word.begin(),word.end(),word.begin(),::tolower);
					if(stopWordsIndex[word])
					{
						token = strtok(NULL,"\n ,-.:;?!");
						continue;
					}
					if(localIndex[word].size() == 0)
					{
						vector<docFreq> docWordFreq;
						localIndex[word] = docWordFreq;
					}
					if(!isAlreadyPresent[word])
					{
						isAlreadyPresent[word] = true;
						docFreq d1;
						d1.docID = docID;
						d1.frequency = 1;
						localIndex[word].push_back(d1);
					}
					else
					{
						vector<docFreq>::iterator itr = localIndex[word].end();
						itr--;
						itr->frequency++;
					}
					token = strtok(NULL,"\n ,-.:;?!\"");
				}
			}
			
			doc.close();
		}
		closedir(dir);
	}
	map<string,vector<docFreq> >::iterator mapItr;
	for(mapItr = localIndex.begin(); mapItr != localIndex.end(); mapItr++)
	{
		sort((mapItr->second).begin(),(mapItr->second).end(),compareDocFreq);
	}
	return localIndex;
}

void printLocalIndex(map<string,vector<docFreq> > localIndex)
{
	map<string,vector<docFreq> >::iterator mapItr;
	for(mapItr = localIndex.begin(); mapItr != localIndex.end(); mapItr++)
	{
		cout << mapItr->first << ":\t";
		vector<docFreq>::iterator itr;
		for(itr = (mapItr->second).begin(); itr != (mapItr->second).end(); itr++)
		{
			cout << itr->docID << ":" << itr->frequency << "  ";
		}
		cout << "\n";
	}
}

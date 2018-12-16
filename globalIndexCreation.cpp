#include "localIndexCreation.cpp"

serializedMap serializeLocalMap(map<string, vector<docFreq> > localIndex)
{
	serializedMap s_map;
	map<string, vector<docFreq> >::iterator map_itr;
	int wordLen = -1;
	int vectorLen = -1;
	for(map_itr = localIndex.begin(); map_itr != localIndex.end(); map_itr++)
	{
		string word = map_itr->first;
		wordLen += word.length();
		s_map.indexOfStrings.push_back(wordLen);
		copy(word.begin(),word.end(),back_inserter(s_map.charsInWords));
		vector<docFreq>::iterator vec_itr;

		for(vec_itr = (map_itr->second).begin(); vec_itr != (map_itr->second).end(); vec_itr++)
		{
			docFreq doc = *vec_itr;
			s_map.docIDArr.push_back(doc.docID);
			s_map.FreqArr.push_back(doc.frequency);
		}
		vectorLen += (map_itr->second).size();
		s_map.indexDocFreq.push_back(vectorLen);
	}
	return s_map;
}

map<string, pair<int,int> > deserializeMap(serializedMap s_map_recv)
{
	map<string, pair<int,int> > de_map_recv;// de serializing the received map
	int i;
	vector<int> indexOfStrings = s_map_recv.indexOfStrings;
	vector<char> charsInWords = s_map_recv.charsInWords;
	vector<int> indexDocFreq = s_map_recv.indexDocFreq;
	vector<char>::iterator char_itr = charsInWords.begin();
	int word_start_index = 0;
	int st_index = 0;
	int end_index = 0;
	for(i = 0; i < indexOfStrings.size(); i++)
	{
		string word(char_itr + word_start_index, char_itr + indexOfStrings[i] + 1);
		end_index = indexDocFreq[i];
		de_map_recv[word] = make_pair(st_index,end_index);// indexOfStrings.size() = indexDocFreq.size() && docIdarr.size() == FreqArr.size()
		// printf("%s (%d,%d)\n",word.c_str(),st_index,end_index);
		st_index = end_index + 1;
		word_start_index = indexOfStrings[i] + 1;
	}
	return de_map_recv;
}

map<string,vector<docFreq> > mergeHashMaps(map<string,vector<docFreq> > localIndex,map<string, pair<int,int> > de_map_recv,vector<int> docIDArr,vector<int> FreqArr,int my_rank,int level,int num_processes)
{
	MPI_Status status;
	// smaller of the two will be the number of checkpoints - this is because there will be lesser communication
	int pow_2_level = (int)pow(2,level);
	int pow_2_level_minus = (int)pow(2,level-1);
	int de_map_recv_size = de_map_recv.size();
	map<string,vector<docFreq> > mergedHashMapHalf;
	int numCheckpoints = localIndex.size() < de_map_recv.size() ? (int)log2(localIndex.size()) : (int)log2(de_map_recv.size());
	int i;
	if(my_rank % pow_2_level == 0)
	{
		vector<int> checkpointIndices;
		int gapBtwCheckPts = de_map_recv_size / numCheckpoints;
		int index = 0;
		while(index < de_map_recv_size)
		{
			checkpointIndices.push_back(index);
			index += gapBtwCheckPts;
		}
		if(index >= de_map_recv_size)
		{
			checkpointIndices.push_back(de_map_recv_size-1);
		}
		map<string,pair<int,int> >::iterator recv_itr;
		index = 0; // index in de_map_recv
		int j = 0; // index in checkpointIndices
		int st_index,end_index; // in de_map_recv
		string word;
		for(recv_itr = de_map_recv.begin(); recv_itr != de_map_recv.end(); recv_itr++)
		{
			int flag = 0;
			st_index = (recv_itr->second).first;
			end_index = (recv_itr->second).second;
			word = recv_itr->first;
			if(index == checkpointIndices[j])
			{
				j++;
				MPI_Send(word.c_str(),word.length(),MPI_BYTE,(my_rank + pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD);
				MPI_Recv(&flag,1,MPI_INT,(my_rank + pow_2_level_minus)%num_processes,1,MPI_COMM_WORLD,&status);
			}
			vector<docFreq> recvWordDocs; // vector of docFreq of the received word
			for(i = st_index; i <= end_index; i++)
			{
				docFreq doc;
				doc.docID = docIDArr[i];
				doc.frequency = FreqArr[i];
				recvWordDocs.push_back(doc);
			}

			if(localIndex[word].size() > 0) // word present in localIndex
			{
				vector<docFreq> mergedVec = mergeVectors(localIndex[word],recvWordDocs);
				mergedHashMapHalf[word] = mergedVec;
			}
			else
			{
				mergedHashMapHalf[word] = recvWordDocs;
			}
			localIndex.erase(word);
			if(flag == 1)
			{
				break;
			}
			index++;
		}
		map<string,vector<docFreq> >::iterator local_itr;
		for(local_itr = localIndex.begin(); local_itr != localIndex.end(); local_itr++)
		{
			if((local_itr->first).compare(word) >= 0)
			{
				break;
			}
			mergedHashMapHalf[local_itr->first] = local_itr->second;
		}
	}
	else
	{
		vector<int> checkpointIndices;
		int gapBtwCheckPts = de_map_recv_size / numCheckpoints;
		int index = de_map_recv_size - 1;
		while(index > 0)
		{
			checkpointIndices.push_back(index);
			index -= gapBtwCheckPts;
		}
		if(index <= 0)
		{
			checkpointIndices.push_back(0);
		}
		map<string,pair<int,int> >::reverse_iterator recv_ritr;
		index = de_map_recv_size - 1; // index in de_map_recv
		int j = 0; // index in checkpointIndices
		int st_index,end_index; // in de_map_recv
		string word,str_check_word;
		for(recv_ritr = de_map_recv.rbegin(); recv_ritr != de_map_recv.rend(); recv_ritr++)
		{
			int flag = 0;
			st_index = (recv_ritr->second).first;
			end_index = (recv_ritr->second).second;
			char check_word[30];
			word = recv_ritr->first;
			if(index == checkpointIndices[j])
			{
				j++;
				MPI_Recv(check_word,30,MPI_BYTE,(my_rank - pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD,&status);
				str_check_word = string(check_word);
				if(word.compare(str_check_word) <= 0)
				{
					flag = 1;
				}
				MPI_Send(&flag,1,MPI_INT,(my_rank - pow_2_level_minus)%num_processes,1,MPI_COMM_WORLD);
			}
			if(flag == 1)
			{
				map<string,vector<docFreq> >::iterator mainMap_itr;// back track
				for(mainMap_itr = mergedHashMapHalf.begin(); mainMap_itr != mergedHashMapHalf.end(); mainMap_itr++)
				{
					if((mainMap_itr->first).compare(str_check_word) <= 0)
					{
						mergedHashMapHalf.erase(mainMap_itr);
					}
					else
					{
						break;
					}
				}
				break;
			}
			vector<docFreq> recvWordDocs; // vector of docFreq of the received word
			for(i = st_index; i <= end_index; i++)
			{
				docFreq doc;
				doc.docID = docIDArr[i];
				doc.frequency = FreqArr[i];
				recvWordDocs.push_back(doc);
			}

			if(localIndex[word].size() > 0) // word present in localIndex
			{
				vector<docFreq> mergedVec = mergeVectors(localIndex[word],recvWordDocs);
				mergedHashMapHalf[word] = mergedVec;
			}
			else
			{
				mergedHashMapHalf[word] = recvWordDocs;
			}
			localIndex.erase(word);
			index--;
		}
		map<string,vector<docFreq> >::reverse_iterator local_ritr;
		for(local_ritr = localIndex.rbegin(); local_ritr != localIndex.rend(); local_ritr++)
		{
			if((local_ritr->first).compare(str_check_word) <= 0)
			{
				break;
			}
			mergedHashMapHalf[local_ritr->first] = local_ritr->second;
		}
	}
	return mergedHashMapHalf;
}

vector<docFreq> mergeVectors(vector<docFreq> vec1, vector<docFreq> vec2)
{
	vector<docFreq> mergedVec;
	int i = 0;
	int j = 0;
	while(i < vec1.size() && j < vec2.size())
	{
		if(vec1[i].frequency >= vec2[j].frequency)
		{
			mergedVec.push_back(vec1[i]);
			i++;
		}
		else
		{
			mergedVec.push_back(vec2[j]);
			j++;
		}
	}
	while(i < vec1.size())
	{
		mergedVec.push_back(vec1[i]);
		i++;
	}
	while(j < vec2.size())
	{
		mergedVec.push_back(vec2[j]);
		j++;
	}
	return mergedVec;
}

map<string,vector<docFreq> > deserializeAndMerge(map<string,vector<docFreq> > map1,serializedMap s_map)
{
	int i,j;
	vector<int> indexOfStrings = s_map.indexOfStrings;
	vector<char> charsInWords = s_map.charsInWords;
	vector<int> indexDocFreq = s_map.indexDocFreq;
	vector<char>::iterator char_itr = charsInWords.begin();
	int word_start_index = 0;
	int st_index = 0;
	int end_index = 0;
	for(i = 0; i < indexOfStrings.size(); i++)
	{
		string word(char_itr + word_start_index, char_itr + indexOfStrings[i] + 1);
		end_index = indexDocFreq[i];
		vector<docFreq> recvWordDocs; // vector of docFreq of the received word
		for(j = st_index; j <= end_index; j++)
		{
			docFreq doc;
			doc.docID = s_map.docIDArr[j];
			doc.frequency = s_map.FreqArr[j];
			recvWordDocs.push_back(doc);
		}
		map1[word] = recvWordDocs;
		st_index = end_index + 1;
		word_start_index = indexOfStrings[i] + 1;
	}
	return map1;
}
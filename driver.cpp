#include "globalIndexCreation.cpp"
// document id's on different nodes should have different names

int main(int argc, char *argv[])
{
	MPI_Init(NULL,NULL);
	double startLocalIndexTime, endLocalIndexTime;
	startLocalIndexTime = MPI_Wtime();
	int num_processes;
	MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
	int my_rank;
	MPI_Status status;
	MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
	map<string, bool> stopWordsIndex;
	map<string,vector<docFreq> > localIndex;
	ifstream fp("stopwords.txt");
	string stopWord;

	// create stop words map
	while(getline(fp,stopWord))
	{
		stopWordsIndex[stopWord] = true;
	}
	fp.close();
	// map<string,bool>::iterator stop_itr;
	// for(stop_itr = stopWordsIndex.begin(); stop_itr != stopWordsIndex.end(); stop_itr++)
	// {
	// 	cout << stop_itr->first << "\n" <<  stop_itr->second << "\n";
	// }

	// create local index
	char* dirPath = argv[1];
	localIndex = createLocalIndex(dirPath,stopWordsIndex,my_rank);
	// printLocalIndex(localIndex);
	endLocalIndexTime = MPI_Wtime();

	printf("Time taken for local Indexing by process %d:  %lf seconds\n", my_rank, endLocalIndexTime - startLocalIndexTime);

	// write local index to file
	map<string, vector<docFreq> >::iterator map_itr;
	string file_name = "localIndex" + to_string(my_rank) + ".txt";
	ofstream localIndexFile(file_name,ios::out);
	for(map_itr = localIndex.begin(); map_itr != localIndex.end(); map_itr++)
	{
		localIndexFile << (map_itr->first + ": ");
		vector<docFreq>::iterator vec_itr;
		for(vec_itr = (map_itr->second).begin(); vec_itr != (map_itr->second).end(); vec_itr++)
		{
			localIndexFile << ("("+to_string(vec_itr->docID)+", "+to_string(vec_itr->frequency)+")  ");
		}
		localIndexFile << "\n";
	}

	// do parallel processing only if number of processes > 1 and number of processes is a power of 2
	if(num_processes == 1)
	{
		MPI_Finalize();
		return 0;
	}
	else if(ceil(log2(num_processes)) != floor(log2(num_processes)))
	{
		printf("Give number of processes as power of 2 for better scalability\n");
		MPI_Finalize();
		return 0;
	}

	// synchronization point
	MPI_Barrier(MPI_COMM_WORLD);
	
	// global merging
	double startGlobalIndexTime, endGlobalIndexTime;

	startGlobalIndexTime = MPI_Wtime();
	int height_tree = (int)log2(num_processes);
	int level = 1;
	map<string,vector<docFreq> > mergedHashMapHalf = localIndex;
	while(level <= height_tree)
	{
		int pow_2_level = (int)pow(2,level);
		int pow_2_level_minus = (int)pow(2,level-1);
		if(my_rank % pow_2_level_minus == 0)
		{
			serializedMap s_map_send;
			// serialize the index in a process created so far
			s_map_send = serializeLocalMap(mergedHashMapHalf);
			serializedMap s_map_recv;
			vector<int> sizes;
			sizes.clear();
			// send sizes of vectors so that on receiver side, memory is allocated for the vectors(buffers)
			sizes.push_back(s_map_send.charsInWords.size());
			sizes.push_back(s_map_send.indexOfStrings.size());
			sizes.push_back(s_map_send.docIDArr.size());
			vector<int> sizes_recv(3);
			// half of the processes will send first then receive
			if(my_rank % pow_2_level == 0)
			{
				MPI_Send(&sizes[0],sizes.size(),MPI_INT,(my_rank + pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.charsInWords[0],s_map_send.charsInWords.size(),MPI_BYTE,(my_rank+pow_2_level_minus)%num_processes,1,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.indexOfStrings[0],s_map_send.indexOfStrings.size(),MPI_INT,(my_rank+pow_2_level_minus)%num_processes,2,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.docIDArr[0],s_map_send.docIDArr.size(),MPI_INT,(my_rank+pow_2_level_minus)%num_processes,3,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.FreqArr[0],s_map_send.FreqArr.size(),MPI_INT,(my_rank+pow_2_level_minus)%num_processes,4,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.indexDocFreq[0],s_map_send.indexDocFreq.size(),MPI_INT,(my_rank+pow_2_level_minus)%num_processes,5,MPI_COMM_WORLD);


				MPI_Recv(&sizes_recv[0],3,MPI_INT,(my_rank+pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD,&status);
				s_map_recv.charsInWords.resize(sizes_recv[0]);
				s_map_recv.indexOfStrings.resize(sizes_recv[1]);
				s_map_recv.indexDocFreq.resize(sizes_recv[1]);
				s_map_recv.docIDArr.resize(sizes_recv[2]);
				s_map_recv.FreqArr.resize(sizes_recv[2]);
				MPI_Recv(&s_map_recv.charsInWords[0], s_map_recv.charsInWords.size(), MPI_BYTE, (my_rank+pow_2_level_minus)%num_processes, 1, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.indexOfStrings[0], s_map_recv.indexOfStrings.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 2, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.docIDArr[0], s_map_recv.docIDArr.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 3, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.FreqArr[0], s_map_recv.FreqArr.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 4, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.indexDocFreq[0], s_map_recv.indexDocFreq.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 5, MPI_COMM_WORLD, &status);

			}
			// half of the processes will receive first then send
			else
			{
				MPI_Recv(&sizes_recv[0],3,MPI_INT,(my_rank-pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD,&status);
				s_map_recv.charsInWords.resize(sizes_recv[0]);
				s_map_recv.indexOfStrings.resize(sizes_recv[1]);
				s_map_recv.indexDocFreq.resize(sizes_recv[1]);
				s_map_recv.docIDArr.resize(sizes_recv[2]);
				s_map_recv.FreqArr.resize(sizes_recv[2]);
				MPI_Recv(&s_map_recv.charsInWords[0], s_map_recv.charsInWords.size(), MPI_BYTE, (my_rank-pow_2_level_minus)%num_processes, 1, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.indexOfStrings[0], s_map_recv.indexOfStrings.size(), MPI_INT, (my_rank-pow_2_level_minus)%num_processes, 2, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.docIDArr[0], s_map_recv.docIDArr.size(), MPI_INT, (my_rank-pow_2_level_minus)%num_processes, 3, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.FreqArr[0], s_map_recv.FreqArr.size(), MPI_INT, (my_rank-pow_2_level_minus)%num_processes, 4, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.indexDocFreq[0], s_map_recv.indexDocFreq.size(), MPI_INT, (my_rank-pow_2_level_minus)%num_processes, 5, MPI_COMM_WORLD, &status);

				MPI_Send(&sizes[0],sizes.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.charsInWords[0],s_map_send.charsInWords.size(),MPI_BYTE,(my_rank-pow_2_level_minus)%num_processes,1,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.indexOfStrings[0],s_map_send.indexOfStrings.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,2,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.docIDArr[0],s_map_send.docIDArr.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,3,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.FreqArr[0],s_map_send.FreqArr.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,4,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.indexDocFreq[0],s_map_send.indexDocFreq.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,5,MPI_COMM_WORLD);
			}

			// deserialize the map received
			map<string, pair<int,int> > de_map_recv = deserializeMap(s_map_recv);
			
			// map<string, pair<int,int> >::iterator de_itr;
			// for(de_itr = de_map_recv.begin(); de_itr != de_map_recv.end(); de_itr++)
			// {
			// 	printf("%s :(%d,%d) process:%d\n",(de_itr->first).c_str(),(de_itr->second).first,(de_itr->second).second,my_rank);
			// }

			// merge the local map and the map received from neighbour process create half a final map
			mergedHashMapHalf = mergeHashMaps(mergedHashMapHalf,de_map_recv,s_map_recv.docIDArr,s_map_recv.FreqArr,my_rank,level,num_processes); // other half will be sent by neighbour process
			
			// map<string,vector<docFreq> >::iterator mh_itr;
			// for(mh_itr = mergedHashMapHalf.begin(); mh_itr != mergedHashMapHalf.end(); mh_itr++)
			// {
			// 	// printf("%s(%d) :\n",mh_itr->first,my_rank);
			// 	vector<docFreq>::iterator v_itr;
			// 	for(v_itr = (mh_itr->second).begin(); v_itr != (mh_itr->second).end();v_itr++)
			// 	{
			// 		printf("%s(%d)(%d) process:%d\n",(mh_itr->first).c_str(),v_itr->docID,v_itr->frequency,my_rank);
			// 	}
			// }

			// one process will send the map after serializing it
			if(my_rank%pow_2_level != 0)
			{
				s_map_send = serializeLocalMap(mergedHashMapHalf);
				sizes.clear();
				sizes.push_back(s_map_send.charsInWords.size());
				sizes.push_back(s_map_send.indexOfStrings.size());
				sizes.push_back(s_map_send.docIDArr.size());
				MPI_Send(&sizes[0],sizes.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.charsInWords[0],s_map_send.charsInWords.size(),MPI_BYTE,(my_rank-pow_2_level_minus)%num_processes,1,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.indexOfStrings[0],s_map_send.indexOfStrings.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,2,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.docIDArr[0],s_map_send.docIDArr.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,3,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.FreqArr[0],s_map_send.FreqArr.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,4,MPI_COMM_WORLD);
				MPI_Send(&s_map_send.indexDocFreq[0],s_map_send.indexDocFreq.size(),MPI_INT,(my_rank-pow_2_level_minus)%num_processes,5,MPI_COMM_WORLD);
			}

			// other process will receive the map and do combining of the 2 half maps 
			else
			{
				MPI_Recv(&sizes_recv[0],3,MPI_INT,(my_rank+pow_2_level_minus)%num_processes,0,MPI_COMM_WORLD,&status);
				s_map_recv.charsInWords.resize(sizes_recv[0]);
				s_map_recv.indexOfStrings.resize(sizes_recv[1]);
				s_map_recv.indexDocFreq.resize(sizes_recv[1]);
				s_map_recv.docIDArr.resize(sizes_recv[2]);
				s_map_recv.FreqArr.resize(sizes_recv[2]);
				MPI_Recv(&s_map_recv.charsInWords[0], s_map_recv.charsInWords.size(), MPI_BYTE, (my_rank+pow_2_level_minus)%num_processes, 1, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.indexOfStrings[0], s_map_recv.indexOfStrings.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 2, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.docIDArr[0], s_map_recv.docIDArr.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 3, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.FreqArr[0], s_map_recv.FreqArr.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 4, MPI_COMM_WORLD, &status);
				MPI_Recv(&s_map_recv.indexDocFreq[0], s_map_recv.indexDocFreq.size(), MPI_INT, (my_rank+pow_2_level_minus)%num_processes, 5, MPI_COMM_WORLD, &status);
				
				mergedHashMapHalf = deserializeAndMerge(mergedHashMapHalf,s_map_recv);

			}
		}
		level++;
	}
	endGlobalIndexTime = MPI_Wtime();
	// write global index in a file
	if(my_rank == 0)
	{
		printf("\nTime taken for creating global index %lf seconds\n", endGlobalIndexTime - startGlobalIndexTime);
		file_name = "globalIndex.txt";
		ofstream globalIndexFile(file_name,ios::out);
		for(map_itr = mergedHashMapHalf.begin(); map_itr != mergedHashMapHalf.end(); map_itr++)
		{
			globalIndexFile << (map_itr->first + ": ");
			vector<docFreq>::iterator vec_itr;
			for(vec_itr = (map_itr->second).begin(); vec_itr != (map_itr->second).end(); vec_itr++)
			{
				globalIndexFile << ("("+to_string(vec_itr->docID)+", "+to_string(vec_itr->frequency)+")  ");
			}
			globalIndexFile << "\n";
		}
	}
	MPI_Finalize();
	return 0;
}

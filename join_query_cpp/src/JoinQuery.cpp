#include "JoinQuery.hpp"
#include <assert.h>
#include <fstream>
#include <thread>
#include <iostream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <vector>
#include <unordered_set>
#include <sys/types.h>
#include <unistd.h>
#include <set>
//---------------------------------------------------------------------------
JoinQuery::JoinQuery(std::string lineitem, std::string order,
                     std::string customer)
{
   lineitem_constr = lineitem;
   orders_constr = order;
   customer_constr = customer;
}
//---------------------------------------------------------------------------
size_t JoinQuery::avg(std::string segmentParam)
{
   int handler = open(customer_constr.c_str(), O_RDONLY);
   if (handler<0){
      std::cerr <<"unable to open"<<customer_constr<<std::endl;
      return 1;
   }

   long file_size = lseek(handler, 0, SEEK_END);
   //allocate memory
   auto file_start =  static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_SHARED, handler, 0));
   auto file_end = file_start + file_size;
   int o_first, o_second; 

   std::unordered_set<int> customers_value;
   std::vector<std::string> index_content;
   std::set<int> cust_index = {0,6};

   auto line_start = file_start;
   auto line_end = file_start;

   while((line_end != file_end) && (line_start != file_end)){
      line_end = (char*) memchr(line_start, '\n', line_start - file_end);
      char line_new[line_end - line_start];
      memcpy(line_new, line_start, line_end-line_start);
      std::string line_new_str(line_new);
      index_content = getContent(&line_new_str, &cust_index);
      o_first  = std::stoi(index_content[0]);
      if (index_content[1] == segmentParam)
         customers_value.insert(o_first);
      line_start = line_end +1;
   }
   
   if(munmap(file_start, file_size)==-1)
   {
      close(handler);
      perror("Error un-mapping the file");
      exit(EXIT_FAILURE);
   }

   close(handler);

   std::set<int> orders_index = {0,1};
   std::unordered_set<int> orders_value; 

   handler = open(orders_constr.c_str(), O_RDONLY);
   if(handler<0){
      std::cerr << "unable to open" << orders_constr << std::endl;
      return 1;
   }

   file_size = lseek(handler, 0, SEEK_END);
   file_start = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_SHARED, handler, 0));
   file_end = file_start + file_size;

   line_start = file_start;
   line_end = file_start;
   while((line_end != file_end) && (line_start!=file_end)){
      line_end = (char*) memchr(line_start, '\n', line_start - file_end);
      char line_new[line_end-line_start];
      memcpy(line_new, line_start, line_end - line_start);
      std::string line_new_str(line_new);
      index_content = getContent(&line_new_str, &orders_index);
      o_first = std::stoi(index_content[0]);
      o_second = std::stoi(index_content[1]);
      if(customers_value.find(o_second) != customers_value.end()){
         orders_value.insert(o_first);
      }
      line_start = line_end +1;
   }

   if(munmap(file_start, file_size) == -1){
      close(handler);
      perror("Error un-mapping the file");
      exit(EXIT_FAILURE);
   }

   close(handler);


   uint64_t average = 0;
   uint64_t counter = 0;
   handler = open(lineitem_constr.c_str(), O_RDONLY);
   if(handler<0){
      std::cerr << "unable to open" << lineitem_constr << std::endl;
      return 1;
   }

   file_size = lseek(handler, 0, SEEK_END);
   file_start = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_SHARED, handler, 0));
   file_end = file_start + file_size;

   unsigned threadnums = std::thread::hardware_concurrency();
   auto thread_bounds = [file_start, threadnums, file_size](unsigned index){
      return file_start+(index*file_size/threadnums);
   };
   std::vector<std::thread> threads;
   for(unsigned index=0; index!=threadnums;index++){
      threads.push_back(std::thread([&average, &counter, &thread_bounds, file_end, index, &orders_value, this](){
         auto result = this->computeAverage(thread_bounds(index), thread_bounds(index+1), file_end, &orders_value, index);
         average += result.first;
         counter += result.second;
      }));
   }


   for(auto& t:threads) t.join();

   if(munmap(file_start, file_size) == -1){
      close(handler);
      perror("Error un-mapping the file");
      exit(EXIT_FAILURE);
   }

   close(handler);

   return average*100/counter;
}

std::vector<std::string> JoinQuery::getContent(std::string* s, std::set<int>* index){
   std::string delims = "|";
   size_t position = 0;
   int counter = 0;
   int counter_position = 0;
   std::string token;
   std::vector<std::string> result;
   result.reserve(index->size());
   while(((position = s->find(delims)) != std::string::npos) && (counter_position != 2)){
      token = s->substr(0,position);
      if(index->find(counter)!=index->end()){
         result.push_back(token);
         counter_position++;
      }

      s->erase(0, position+delims.length());
      counter++;
   }
   return result;
};

std::pair<uint64_t, uint64_t> JoinQuery::computeAverage(char* partition_start, char* partition_end, char* file_end, std::unordered_set<int>* orders_value, unsigned index){
   std::vector<std::string> content_index;
   std::set<int> l_index = {0,4};
   int o_first;
   uint64_t o_second;
   uint64_t average = 0;
   uint64_t counter = 0;

   if(index==0){
      partition_end = (char*) memchr(partition_end, '\n', partition_end- file_end);

   }else{
      partition_start = (char*) memchr(partition_start, '\n', partition_start-partition_end)+1;
      if(partition_end == file_end){
         partition_end = file_end;
      }else
      {
         partition_end = (char*) memchr(partition_end, '\n', partition_end-file_end);
      }
      
   }

   auto line_start = partition_start;
   auto line_end = partition_start;

   while((line_end!=partition_end) && (line_start!=partition_end)){
      line_end = (char*) memchr(line_start, '\n', line_start-partition_end);
      char line_new[line_end-line_start];
      memcpy(line_new, line_start, line_end-line_start);
      std::string line_new_string(line_new);
      content_index = getContent(&line_new_string, &l_index);
      o_first = std::stoi(content_index[0]);
      o_second = (uint64_t) std::stod(content_index[1]);
      if(orders_value->find(o_first) != orders_value->end()){
         average += o_second;
         counter++;
      }

      line_start = line_end + 1;
   
   }
   return std::pair<uint64_t, uint64_t>(average, counter);
   
}
//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------

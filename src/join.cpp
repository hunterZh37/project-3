#include "join.hpp"

#include <array>
#include <cstdint>
#include <vector>
#include <iostream>

JoinAlgorithm getJoinAlgorithm()
{
  // TODO: Return your chosen algorithm.
  return JOIN_ALGORITHM_BNLJ;
  // return JOIN_ALGORITHM_SMJ;
  // return JOIN_ALGORITHM_HJ;
  // throw std::runtime_error("not implemented: getJoinAlgorithm");
};


/*
  R   void *ptr_at_first_R = (void *) (buffer);
  R   void *ptr_at_second_R = (void *) (buffer + 1 * 512 * 8);
  S   void *ptr_at_S = (void *) (buffer + 2 * 512 * 8);
  Out void *ptr_at_Out = (void *) (buffer + 3 * 512 * 8);
*/

int join(File &file, int numPagesR, int numPagesS, char *buffer,
         int numFrames)
{
  // TODO: Implement your chosen algorithm.
  // Currently, this function performs a nested loop join in memory. While it
  // will pass the correctness and I/O cost tests, it ignores the provided
  // buffer and instead reads table data into vectors. It will not satisfy the
  // constraints on peak heap memory usage for large input tables. You should
  // delete this code and replace it with your disk-based join algorithm
  // implementation.

  int pageIndexR = 0;                         //initial page index for R
  int pageIndexS = pageIndexR + numPagesR;    //initial page index for S
  int pageIndexOut = pageIndexS + numPagesS;  //initial page index for tuplesOut
  int numPagesRUnChecked = numPagesR;
  int internalPageIndexS = pageIndexS;        //internal page index for S to iterate through R pages
  int internalPageIndexR = pageIndexR;         //internal page index for R to iterate through S pages


  std::vector<Tuple> tuplesOut;
  Tuple *tuplesR;
  Tuple *tuplesS;

  while (numPagesRUnChecked != 0)
  {
    if (numPagesRUnChecked < numFrames - 2)
    { // only one page left to check
      file.read(buffer, internalPageIndexR, numPagesRUnChecked);
      tuplesR = (Tuple *)buffer;

      //       for (int i = 0; i < 512 ; i++) {
      // Tuple *tuple = &tuplesR[i];
      // u_int32_t firstInt = std::get<0>(*tuple);
      // u_int32_t secondInt = std::get<1>(*tuple);
      // std::cout << "R(" << firstInt << ", " << secondInt << ")" << std::endl;
      // }
      
      void *ptr_at_S = (void *)(buffer + (numPagesRUnChecked) * 512 * 8);
      internalPageIndexS = pageIndexS;
      for (int i = 0; i < numPagesS; i++)
      {
        file.read((char *)ptr_at_S, internalPageIndexS, 1);
        tuplesS = (Tuple *)ptr_at_S;

        //            for (int i = 0; i < 512 ; i++) {
        // Tuple *tuple = &tuplesS[i];
        // u_int32_t firstInt = std::get<0>(*tuple);
        // u_int32_t secondInt = std::get<1>(*tuple);
        // std::cout << "S(" << firstInt << ", " << secondInt << ")" << std::endl;
        // }

        for (int s = 0; s < 512; s++)
        {
          Tuple *tupleS = &tuplesS[s];
          u_int32_t sFirst = std::get<0>(*tupleS);
          u_int32_t sSecond = std::get<1>(*tupleS);

          for (int r = 0; r < 512 * numPagesRUnChecked; r++)
          {
            Tuple *tupleR = &tuplesR[r];
            u_int32_t rFirst = std::get<0>(*tupleR);
            u_int32_t rSecond = std::get<1>(*tupleR);
            if (rFirst == sFirst)
            {
              tuplesOut.emplace_back(rSecond, sSecond);
              break;
            }
          }
        }

        // for (size_t s = 0; s < tuplesOut.size(); s++)
        // {
        //   u_int32_t firstInt = std::get<0>(tuplesOut[s]);
        //   u_int32_t secondInt = std::get<1>(tuplesOut[s]);
        //    std::cout << "out1(" << firstInt << ", " << secondInt << ")" << std::endl;
        // }
        internalPageIndexS += 1;
      }

      
     
      numPagesRUnChecked -= numPagesRUnChecked;
    }
    else // more than one pages left to check
    {    
      file.read(buffer, internalPageIndexR, numFrames - 2);
      tuplesR = (Tuple *)buffer;
      

      // for (int i = 0; i < 512 * 2 ; i++) {
      //     Tuple *tuple = &tuplesR[i];
      //     u_int32_t firstInt = std::get<0>(*tuple);
      //     u_int32_t secondInt = std::get<1>(*tuple);
      //     std::cout << "R(" << firstInt << ", " << secondInt << ")" << std::endl;
      // }




      void *ptr_at_S = (void *)(buffer + (numFrames-2) * 512 * 8);
      internalPageIndexS = pageIndexS;

      for (int i = 0; i < numPagesS; i++)
      {
        file.read((char *)ptr_at_S, internalPageIndexS, 1);
        tuplesS = (Tuple *)ptr_at_S;

        //            for (int i = 0; i < 512 ; i++) {
        // Tuple *tuple = &tuplesS[i];
        // u_int32_t firstInt = std::get<0>(*tuple);
        // u_int32_t secondInt = std::get<1>(*tuple);
        // std::cout << "S(" << firstInt << ", " << secondInt << ")" << std::endl;
        // }

        for (int s = 0; s < 512 ; s++)
        {
          Tuple *tupleS = &tuplesS[s];
          u_int32_t sFirst = std::get<0>(*tupleS);
          u_int32_t sSecond = std::get<1>(*tupleS);
         // std::cout << "S(" << sFirst << ", " << sSecond << ")" << std::endl;

          for (int r = 0; r < 512 * (numFrames-2); r++)
          {
            Tuple *tupleR = &tuplesR[r];
            u_int32_t rFirst = std::get<0>(*tupleR);
            u_int32_t rSecond = std::get<1>(*tupleR);
            // std::cout << "R(" << rFirst << ", " << rSecond << ")" << std::endl;
            if (rFirst == sFirst && rFirst != 0)
            {    
              tuplesOut.emplace_back(rSecond, sSecond);
              break;
            }
          }
        }

       internalPageIndexS += 1;
      }     

      numPagesRUnChecked -= (numFrames-2);
      internalPageIndexR += (numFrames-2);    
    }
    
  }

//-------------------------------------------------------------------------------------------- debug log open
  // std::sort(tuplesOut.begin(), tuplesOut.end());

  //  for (size_t s = 0; s < tuplesOut.size(); s++)
  //       {
  //         u_int32_t firstInt = std::get<0>(tuplesOut[s]);
  //         u_int32_t secondInt = std::get<1>(tuplesOut[s]);
  //          std::cout << "out1(" << firstInt << ", " << secondInt << ")" << std::endl;
  //       }
//-------------------------------------------------------------------------------------------- debug log closed
  int numTuplesOut = (int)tuplesOut.size();
  int numPagesOut = numTuplesOut / 512 + (numTuplesOut % 512 != 0);
  file.write(tuplesOut.data(), pageIndexOut, numPagesOut);

  return numTuplesOut;
}


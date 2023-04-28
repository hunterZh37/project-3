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
  int internalPageIndexR = pageIndexR;        //internal page index for R to iterate through S pages
  int numCurrentPagesLeft = 0;


  std::vector<Tuple> tuplesOut;
  Tuple *tuplesR;
  Tuple *tuplesS;

  void *ptr_at_S;

  u_int32_t sFirst;
  u_int32_t sSecond;
  u_int32_t rFirst;
  u_int32_t rSecond;

  while (numPagesRUnChecked != 0)
  {
    if (numPagesRUnChecked < numFrames - 2)
    { // only one page left to check
      numCurrentPagesLeft = numPagesRUnChecked;
    }
    else // more than one pages left to check
    {
      numCurrentPagesLeft = numFrames - 2; 
    }

    // same for either case
    file.read(buffer, internalPageIndexR, numCurrentPagesLeft);
    tuplesR = (Tuple *)buffer;

    ptr_at_S = (void *)(buffer + (numCurrentPagesLeft) * 512 * 8);
    internalPageIndexS = pageIndexS;
    for (int i = 0; i < numPagesS; i++)
    {
      file.read((char *)ptr_at_S, internalPageIndexS, 1);
      tuplesS = (Tuple *)ptr_at_S;
      
      for (int s = 0; s < 512; s++)
      {
        sFirst = std::get<0>(tuplesS[s]);
        sSecond = std::get<1>(tuplesS[s]);
        for (int r = 0; r  < 512 * numCurrentPagesLeft; r++)
        {
          rFirst = std::get<0>(tuplesR[r]);
          rSecond = std::get<1>(tuplesR[r]);

          if (rFirst == sFirst) {
            tuplesOut.emplace_back(rSecond, sSecond);
            break;
          }
        }
      }
        internalPageIndexS += 1;
    }

    if (numPagesRUnChecked < numFrames - 2)
    {
      numPagesRUnChecked -= numCurrentPagesLeft;
    }
    else
    {
      numPagesRUnChecked -= numCurrentPagesLeft;
      internalPageIndexR += numCurrentPagesLeft;
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
  // std::cout << "numTuplesOut = " << numTuplesOut << std::endl;
  file.write(tuplesOut.data(), pageIndexOut, numPagesOut);

  return numTuplesOut;
}


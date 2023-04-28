#include "join.hpp"

#include <cstdint>
#include <vector>
#include <iostream>

JoinAlgorithm getJoinAlgorithm()
{
  return JOIN_ALGORITHM_BNLJ;
};

int join(File &file, int numPagesR, int numPagesS, char *buffer,
         int numFrames)
{
  
  int pageIndexR = 0;                         //initial page index for R
  int pageIndexS = pageIndexR + numPagesR;    //initial page index for S
  int pageIndexOut = pageIndexS + numPagesS;  //initial page index for tuplesOut
  int numPagesRUnChecked = numPagesR;
  int internalPageIndexS = pageIndexS;        //internal page index for S to iterate through R pages
  int internalPageIndexR = pageIndexR;        //internal page index for R to iterate through S pages
  int internalPageIndexOut = pageIndexOut;
  int numCurrentPagesLeft = 0;
  int numTuplesOut = 0;

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

          if (rFirst == sFirst and rFirst > 0) {
            tuplesOut.emplace_back(rSecond, sSecond);
            numTuplesOut += 1;
            if ((numTuplesOut % 512) == 0)
            {
              file.write(tuplesOut.data(), internalPageIndexOut, 1);
              internalPageIndexOut += 1;
              tuplesOut.clear();
            }
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

  int numPagesOut = numTuplesOut / 512 + (numTuplesOut % 512 != 0);
  file.write(tuplesOut.data(), internalPageIndexOut, 1);
  tuplesOut.clear();

  return numTuplesOut;
}


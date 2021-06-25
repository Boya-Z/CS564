/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	//Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
	for (FrameId i = 0; i < numBufs; i++) 
  	{
		if(bufDescTable[i].dirty && bufDescTable[i].valid )
		{
			flushFile(bufDescTable[i].file);
		}
  	}

	delete[] bufDescTable;
	delete[] bufPool;
	
}

void BufMgr::advanceClock()
{
	//Advances clock to the next frame in the buffer pool.
	clockHand++;
	clockHand = clockHand % numBufs;
}

//Allocates a free frame using the clock algorithm;
// if necessary, writing a dirty page back to disk. 
//Throws BufferExceededException if all buffer frames are pinned.
// Make sure that if the buffer frame allocated has a valid page in it,
// you remove the appropriate entry from the hash table.
void BufMgr::allocBuf(FrameId & frame) 
{   
    //variable to record number of pinned frames
    uint32_t pinned = 0;
    advanceClock:
	//Throws BufferExceededException if all buffer frames are pinned.
    if(pinned == numBufs)
    {
        throw BufferExceededException();
    }
    advanceClock();

	//Performe ckock algoruthm
    if(!bufDescTable[clockHand].valid)
    {
        //do nothing here
    }
    else
    {
        if(bufDescTable[clockHand].refbit)
        {
            bufDescTable[clockHand].refbit = false;
            goto advanceClock;
        }
        else
        {
            if(bufDescTable[clockHand].pinCnt != 0)
            {
                pinned++;
                goto advanceClock;
            }
            if(bufDescTable[clockHand].dirty)
            {
                flushFile(bufDescTable[clockHand].file);
            }
			else{
            	hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			}
            
        }
    }
	//free the frame and allocate it
	bufDescTable[clockHand].Clear();
    frame = clockHand;
}




void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNum;
	try
	{   
		// Case 2: Page is in the buffer pool. 
		hashTable->lookup(file, pageNo, frameNum);
		bufDescTable[frameNum].refbit = true;
		bufDescTable[frameNum].pinCnt++;
		page = &bufPool[frameNum];
	}
	catch(HashNotFoundException &e)
	{
		//Case 1: Page is not in the buffer pool.
		allocBuf(frameNum);
		bufPool[frameNum] = file->readPage(pageNo);
		hashTable -> insert(file, pageNo, frameNum);
		bufDescTable[frameNum].Set(file, pageNo);
		page = &bufPool[frameNum];
	}	
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frame;
  	try
    {
        // Does nothing if page is not found in the hash table lookup.
        hashTable -> lookup(file, pageNo, frame);
        //Throws PAGENOTPINNED if the pin count is already 0.
        if (bufDescTable[frame].pinCnt == 0)
        {
          throw PageNotPinnedException(file -> filename(), pageNo, frame);
        } 
        //decrements the pinCnt of the frame containing (file,PageNo)
        bufDescTable[frame].pinCnt--;
        //if dirty==true, sets the dirty bit
        if (dirty) 
        {
          bufDescTable[frame].dirty = true;
        }
    }
    catch(HashNotFoundException &e)
    {
    }
}


void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    //allocate an empty page in the specified file by invoking the file->allocatePage() method,return a newly allocated page.
    Page empty;
    empty = file->allocatePage();
    //allocBuf() is called to obtain a buffer pool frame.
    FrameId frame;
    allocBuf(frame);
    //an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly.  
    hashTable ->insert(file,empty.page_number(),frame); //insert ( const File *  file,const PageId  pageNo,const FrameId  frameNo )
    bufDescTable[frame].Set(file, empty.page_number());
	bufPool[frame] = empty;
    //The method returns both the page number of the newly allocated page to the caller via the pageNo parameter 
    pageNo = bufDescTable[frame].pageNo;
    //and a pointer to the buffer frame allocated for the page via the page parameter.
    page = &bufPool[frame];
}

void BufMgr::flushFile(const File* file) 
{
	//Should scan bufTable for pages belonging to the file. 
	for (FrameId i = 0; i < numBufs; i++) 
    {
		if (bufDescTable[i].file == file)
		{
			//For each page encountered it should:
			PageId pageNo = bufDescTable[i].pageNo;
			//Throws BadBuffer-Exception if an invalid page belonging to the file is encountered.
			if(bufDescTable[i].valid == false)
			{
				//BadBufferException(FrameId frameNoIn,bool dirtyIn,bool validIn,bool refbitIn) 
				throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			//Throws PagePinnedException if some page of the file is pinned. 
			if(bufDescTable[i].pinCnt != 0)
			{
				throw PagePinnedException(file -> filename(), pageNo, i);
			}
			// (a) if the page is dirty, call file->writePage() to flush the page to disk 
			if(bufDescTable[i].dirty)
			{
				(bufDescTable[i].file)->writePage(bufPool[i]);
				//and then set the dirty bit for the page to false
				bufDescTable[i].dirty = false;

			}
			//(b) remove the page from the hashtable (whether the page is clean or dirty) 
            hashTable -> remove(file, pageNo);
			//(c) invoke the Clear() method of BufDesc for the page frame.
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	//This method deletes a particular page from file.
	try
	{
		//Before deleting the page from file, it makes sure that if the page to be deleted is allocated a frame in the buffer pool, 
	    FrameId frame;
		//lookup(const File * file,const PageId pageNo,FrameId & frameNo )	
	    hashTable -> lookup(file, PageNo, frame); 
		if (bufDescTable[frame].pinCnt > 0)
		{
			throw PagePinnedException(bufDescTable[frame].file->filename(), bufDescTable[frame].pageNo, bufDescTable[frame].frameNo);
        }
	    //that frame is freed  
		bufDescTable[frame].Clear();
		//correspondingly entry from hash table is also removed.
	    hashTable ->remove(file,PageNo); //remove(const File * file,const PageId pageNo)
	}
	catch(HashNotFoundException &e)
	{
		
	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}

/***
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "file.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"

//#define DEBUG

namespace badgerdb
{

IndexMetaInfo *BTreeIndex::allocateMetaInfoNode(PageId &newPageId)
{
	IndexMetaInfo *newNode;
	bufMgr->allocPage(file, newPageId, (Page *&)newNode);
	return newNode;
}

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset1,
		const Datatype attrType)
{
	bufMgr = bufMgrIn;
	attributeType = attrType;
	attrByteOffset = attrByteOffset1;
	leafOccupancy = 0;
	nodeOccupancy = 0;
    // Add your code below. Please do not remove this line.
    std :: ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
	std :: string indexName = idxStr.str() ; // indexName is the name of the index file
    outIndexName = indexName;


	if(File::exists(outIndexName)) { // Index file exists
	    // NOTE: CHECKING NEEDED!
		// Open the file
		file = new BlobFile(outIndexName, false);
	} else { // Index file does not exist
        // Create a new BlobFile
		file = new BlobFile(outIndexName, true);
	}


    // Initialize the meta info page (First page of index)
	IndexMetaInfo *meta = allocateMetaInfoNode(headerPageNum);
	meta->attrByteOffset = attrByteOffset;
	meta->attrType = attrType;
	strcpy(meta->relationName, relationName.c_str());
	bufMgr->unPinPage(file, headerPageNum, true);

	// TODO: The constructor should scan this relation (using FileScan) and insert 
	// entries for all the tuples in this relation into the index.

	FileScan fscan(relationName, bufMgr);
	try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int *)(record + attrByteOffset));
				// std::cout << "Extracted : " << key << std::endl;
				insertEntry(&key, scanRid);
			}
		}
		catch(const EndOfFileException &e)
		{
			std::cout << "Read all records" << std::endl;
		}

	// filescan goes out of scope here, so relation file gets closed.
	// File::remove(relationName);

    bufMgr->readPage(file, headerPageNum, (Page *&)meta);
	meta->rootPageNo = rootPageNum;
	bufMgr->unPinPage(file, headerPageNum, true);
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.
    if(scanExecuting == true) { // An index scan has been initialized
		endScan();
	}

	// TODO: Clean up any state variables

    // Unpin any B+ tree pages that are pinned and flush the index file
	// try {
	// 	bufMgr->unPinPage(file, rootPageNum, false);
	// } catch (const PageNotPinnedException &e){

	// }
	// try {
	// 	bufMgr->unPinPage(file, 8, false);
	// } catch (const PageNotPinnedException &e){

	// }
	// std::cout << "Before flush file" << std::endl;
	bufMgr->flushFile(file);
    // std::cout << "After flush file" << std::endl;
	// Close the index file (Not deleting)
	delete file;
}

LeafNodeInt *BTreeIndex::allocateLeafNode(PageId &newPageId)
{
	LeafNodeInt *newNode;
	bufMgr->allocPage(file, newPageId, (Page *&)newNode);
	newNode->parentPageNo = -1;
	newNode->rightSibPageNo = -1;
	newNode->numValidKeys = 0;
	return newNode;
}

NonLeafNodeInt *BTreeIndex::allocateNonLeafNode(PageId &newPageId)
{
	NonLeafNodeInt *newNode;
	bufMgr->allocPage(file, newPageId, (Page *&)newNode);
	newNode->parentPageNo = -1;
	newNode->numValidKeys = 0;
	return newNode;
}

void BTreeIndex::searchForLeaf(PageId &targetPageId, int key)
{
	// Situation 1: Tree is empty
	if(leafOccupancy == 0) {
		// TODO: Throw some exception
		throw NoSuchKeyFoundException();

	// Situation 2: Root is of type LEAF node (Root is where to insert)
	} else if(leafOccupancy > 0 && nodeOccupancy == 0) {
		// LeafNodeInt *root;
		// bufMgr->readPage(file, rootPageNum, (Page *&)root);
		targetPageId = rootPageNum;
		return;
	
	// Situation 3: Root is of type NON-LEAF node
	} else if(leafOccupancy > 0 && nodeOccupancy > 0){
		NonLeafNodeInt *curNode;
		// Starting from Root
		bufMgr->readPage(file, rootPageNum, (Page *&)curNode);
		PageId curPageId = rootPageNum; 
		
		int index = 0;
		while(1){
			if(index < curNode->numValidKeys) {
				// 3.1: Found the target index
				if(key < curNode->keyArray[index]) {
					// Next level is leaf
					if(curNode->level == 1) {
						targetPageId = curNode->pageNoArray[index];
						// for(int i=0; i<curNode->numValidKeys; i++) {
						// 	std::cout << "key array of cur Node " << curNode->keyArray[i] << std::endl;
						// }
						// for(int i=0; i<=curNode->numValidKeys; i++) {
						// 	std::cout << "PageNo array of cur Node " << curNode->pageNoArray[i] << std::endl;
						// }
						// std::cout << "A Unpinned non-leaf node of PageId: " << curPageId << std::endl;
						bufMgr->unPinPage(file, curPageId, false);
						return;

					// Next level is non-leaf node
					} else {
						PageId lastPageId = curPageId;
						curPageId = curNode->pageNoArray[index];
						// std::cout << "Unpinned non-leaf node of PageId: " << lastPageId << std::endl;
						bufMgr->unPinPage(file, lastPageId, false);
						bufMgr->readPage(file, curPageId, (Page *&)curNode);
						index = 0;
					}

				// 3.2: key >= root->keyArray[index], go to next
				} else {
					index++;
				}
			} else {
				// Next level is leaf
				if(curNode->level == 1) {
					targetPageId = curNode->pageNoArray[index];
					// if(key == 29) {
					// 	std::cout << "SearchForLeaf(29) gets target ID : " << targetPageId << std::endl;
					// 	Page *target;
					// 	bufMgr->readPage(file, 7, target);
					// 	LeafNodeInt *targetP = (LeafNodeInt *)target;
					// 	for(int i=0; i<(targetP->numValidKeys); i++) {
					// 		std::cout << "targetP 7 key array: " << targetP->keyArray[i] << std::endl;
					// 	}
					// 	bufMgr->unPinPage(file, 7, false);
					// 	bufMgr->readPage(file, 5, target);
					// 	targetP = (LeafNodeInt *)target;
					// 	for(int i=0; i<(targetP->numValidKeys); i++) {
					// 		std::cout << "targetP 5 key array: " << targetP->keyArray[i] << std::endl;
					// 	}
					// 	bufMgr->unPinPage(file, 5, false);
					// 	bufMgr->readPage(file, 8, target);
					// 	targetP = (LeafNodeInt *)target;
					// 	for(int i=0; i<(targetP->numValidKeys); i++) {
					// 		std::cout << "targetP 8 key array: " << targetP->keyArray[i] << std::endl;
					// 	}
					// 	bufMgr->unPinPage(file, 8, false);
					// }
					// std::cout << "Unpinned non-leaf node of PageId: " << curPageId << std::endl;
					bufMgr->unPinPage(file, curPageId, false);
					return;

				// Next level is non-leaf node
				} else {
					PageId lastPageId = curPageId;
					curPageId = curNode->pageNoArray[index];
					bufMgr->unPinPage(file, lastPageId, false);
					bufMgr->readPage(file, curPageId, (Page *&)curNode);
					index = 0;
				}
			}
		}
	}
}

void BTreeIndex::insertToLeaf(PageId leafId, int key, const RecordId rid)
{
	leafOccupancy++;
	LeafNodeInt *curNode;
	bufMgr->readPage(file, leafId, (Page *&)curNode);

	// Situation 0: Leaf empty
	if(curNode->numValidKeys == 0) {
		curNode->keyArray[0] = key;
		curNode->ridArray[0] = rid;
		curNode->numValidKeys++;
		bufMgr->unPinPage(file, leafId, true);
	
	// Situation 1: Leaf not full => Directly insert to the leaf
	} else if(curNode->numValidKeys < INTARRAYLEAFSIZE) {
		int index = 0;
		while(index < curNode->numValidKeys) {
			if(key < curNode->keyArray[index]) {
				// Index to insert at found
				break;
			} else {
				index++;
			}
		}
		
		// Move the nodes at the right side one slot right
		int lastNodeIndex = curNode->numValidKeys - 1;
		for(int i=0; i < (curNode->numValidKeys - index); i++) {
			curNode->keyArray[lastNodeIndex + 1 - i] = curNode->keyArray[lastNodeIndex - i];
			curNode->ridArray[lastNodeIndex + 1 - i] = curNode->ridArray[lastNodeIndex - i];
		}

		// Insert at keyArray[index]
		curNode->keyArray[index] = key;
		curNode->ridArray[index] = rid;
		curNode->numValidKeys++;

		bufMgr->unPinPage(file, leafId, true);

	// Situation 2: Leaf full => Split Leaf Node And go up
	} else {
		// Allocate a new leaf node to the right of curNode
		PageId rightSibPageId;
		LeafNodeInt *rightSib = allocateLeafNode(rightSibPageId);

		// 0 to midIndex - 1 will be allocated to the left
		// midIndex to INTARRAYLEAFSIZE - 1 will be allocated to the right
		int midIndex;
		if(INTARRAYLEAFSIZE % 2 == 0) {
			midIndex = INTARRAYLEAFSIZE / 2 - 1;
		} else {
			midIndex = INTARRAYLEAFSIZE / 2;
		}

		bool insertToLeft = false;
		if(key >= curNode->keyArray[midIndex]) {
			midIndex++;
		} else {
			insertToLeft = true;
		}

		for(int i=0; i<(INTARRAYLEAFSIZE - midIndex); i++) { // Right Sibling
			rightSib->keyArray[i] = curNode->keyArray[midIndex + i];
			rightSib->ridArray[i] = curNode->ridArray[midIndex + i];
			rightSib->numValidKeys++;
			curNode->keyArray[midIndex + i] = 0;
			curNode->ridArray[midIndex + i] = (struct RecordId) {0, 0, 0};
			curNode->numValidKeys--;
		}

		if(insertToLeft) {
			insertToLeaf(leafId, key, rid);
		} else {
			insertToLeaf(rightSibPageId, key, rid);
		}

		// Insert the middle pair of <key, rid> to the parent node
		if(curNode->parentPageNo == std::uint32_t(-1)) {
			allocateNonLeafNode(curNode->parentPageNo);
			bufMgr->unPinPage(file, curNode->parentPageNo, true);
			rightSib->parentPageNo = curNode->parentPageNo;
			rootPageNum = curNode->parentPageNo;
		}
		PageId parentPageNum = curNode->parentPageNo;
		int parentKey = rightSib->keyArray[0];
		// std::cout << "Inserting parent key = " << parentKey << std::endl;
		rightSib->parentPageNo = parentPageNum;
		rightSib->rightSibPageNo = curNode->rightSibPageNo;
		curNode->rightSibPageNo = rightSibPageId;

		bufMgr->unPinPage(file, leafId, true);
		bufMgr->unPinPage(file, rightSibPageId, true);
		insertToNonLeaf(parentPageNum, parentKey, leafId, rightSibPageId, 1);

		// TODO: Add the rightSib and curNode Page number to the parent node's array
	}
}

void BTreeIndex::insertToNonLeaf(PageId nonLeafId, int key, PageId leftChildPageId, PageId rightChildPageId, int level)
{
	nodeOccupancy++;
	NonLeafNodeInt *curNode;
	bufMgr->readPage(file, nonLeafId, (Page *&)curNode);

	// Situation 1: empty node
	if(curNode->numValidKeys == 0) {
		curNode->keyArray[0] = key;
		curNode->pageNoArray[0] = leftChildPageId;
		curNode->pageNoArray[1] = rightChildPageId;
		curNode->numValidKeys++;
		curNode->level = level;
		bufMgr->unPinPage(file, nonLeafId, true);

	// Situation 2: Non-empty node
	} else {
		// 2.1 Node not full => Directly insert
		if(curNode->numValidKeys < INTARRAYNONLEAFSIZE) {
			int index = 0;
			while(index < curNode->numValidKeys) {
				if(key < curNode->keyArray[index]) {
					// Target index found
					break;
				} else {
					index++;
				}
			}
			
			// Move the nodes at the right side one slot right
			int lastNodeIndex = curNode->numValidKeys - 1;
			for(int i=0; i < (curNode->numValidKeys - index); i++) {
				curNode->keyArray[lastNodeIndex + 1 - i] = curNode->keyArray[lastNodeIndex - i];
				curNode->pageNoArray[lastNodeIndex + 2 - i] = curNode->pageNoArray[lastNodeIndex + 1 - i];
			}

			// Insert at keyArray[index]
			curNode->keyArray[index] = key;
			curNode->pageNoArray[index + 1] = rightChildPageId;
			curNode->numValidKeys++;

			bufMgr->unPinPage(file, nonLeafId, true);
		// 2.2: Node Already Full => Split
		} else {
			// Allocate a new non-leaf node to the right of curNode
			PageId rightPageId;
			NonLeafNodeInt *rightPage = allocateNonLeafNode(rightPageId);
			rightPage->level = curNode->level;

			int midIndex;
			if(INTARRAYNONLEAFSIZE % 2 == 0) {
				midIndex = INTARRAYNONLEAFSIZE / 2 - 1;
			} else {
				midIndex = INTARRAYNONLEAFSIZE / 2;
			}
			
			int index = 0;
			while(index < curNode->numValidKeys) {
				if(key < curNode->keyArray[index]) {
					// Target index found
					break;
				} else {
					index++;
				}
			}

			int parentKey;

			// New key will be on the left child node
			if(index <= midIndex) {
				for(int i=0; i<(INTARRAYNONLEAFSIZE - midIndex); i++) {
					rightPage->keyArray[i] = curNode->keyArray[midIndex + i];
					rightPage->pageNoArray[i] = curNode->pageNoArray[midIndex + 1 + i];
					rightPage->numValidKeys++;
					curNode->keyArray[midIndex + i] = 0;
					curNode->pageNoArray[midIndex + 1 + i] = 0;
					curNode->numValidKeys--;
				}
				
				// Move the nodes at the right side one slot right
				int lastNodeIndex = curNode->numValidKeys - 1;
				for(int i=0; i < (curNode->numValidKeys - index); i++) {
					curNode->keyArray[lastNodeIndex + 1 - i] = curNode->keyArray[lastNodeIndex - i];
					curNode->pageNoArray[lastNodeIndex + 2 - i] = curNode->pageNoArray[lastNodeIndex + 1 - i];
				}

				// Insert at keyArray[index]
				curNode->keyArray[index] = key;
				curNode->pageNoArray[index + 1] = rightChildPageId;
				curNode->numValidKeys++;

				// Move the first key on the rightPage to the parent Node
				parentKey = rightPage->keyArray[0];
				for(int i=0; i<(rightPage->numValidKeys - 1); i++) {
					rightPage->keyArray[i] = rightPage->keyArray[i + 1];
				}
				rightPage->numValidKeys--;

			// new key will be the parent node of two child nodes
			} else if(index == midIndex + 1) {
				rightPage->pageNoArray[0] = rightPageId;
				for(int i=0; i<(INTARRAYNONLEAFSIZE - midIndex - 1); i++) {
					rightPage->keyArray[i] = curNode->keyArray[midIndex + 1 + i];
					rightPage->pageNoArray[i + 1] = curNode->pageNoArray[midIndex + 2 + i];
					rightPage->numValidKeys++;
					curNode->keyArray[midIndex + 1 + i] = 0;
					curNode->pageNoArray[midIndex + 2 + i] = 0;
					curNode->numValidKeys--;
				}
				// Add the parent node (key = key)
				parentKey = key;

			// new key will be on the right child node
			} else {
				for(int i=0; i<(INTARRAYNONLEAFSIZE - midIndex - 1); i++) {
					rightPage->keyArray[i] = curNode->keyArray[midIndex + 1 + i];
					rightPage->pageNoArray[i] = curNode->pageNoArray[midIndex + 2 + i];
					rightPage->numValidKeys++;
					curNode->keyArray[midIndex + 1 + i] = 0;
					curNode->pageNoArray[midIndex + 2 + i] = 0;
					curNode->numValidKeys--;
				}

				// Move the nodes at the right side one slot right
				index = index - curNode->numValidKeys;
				int lastNodeIndex = rightPage->numValidKeys - 1;
				for(int i=0; i < (rightPage->numValidKeys - index); i++) {
					rightPage->keyArray[lastNodeIndex + 1 - i] = rightPage->keyArray[lastNodeIndex - i];
					rightPage->pageNoArray[lastNodeIndex + 1 - i] = rightPage->pageNoArray[lastNodeIndex - i];
				}

				// Insert at keyArray[index]
				rightPage->keyArray[index] = key;
				rightPage->pageNoArray[index] = rightChildPageId;
				rightPage->numValidKeys++;

				// Move the leftest node to the parent
				parentKey = rightPage->keyArray[0];
				for(int i=0; i<(rightPage->numValidKeys - 1); i++) {
					rightPage->keyArray[i] = rightPage->keyArray[i+1];
				}
				rightPage->numValidKeys--;
			}

			// Update the parentPageId for rightPage's child pages
			for(int i=0; i<(rightPage->numValidKeys + 1); i++) {
				Page *childNode; 
				bufMgr->readPage(file, rightPage->pageNoArray[i], (Page *&)childNode);
				if(level == 1) {
					LeafNodeInt *child = (LeafNodeInt *)childNode;
					child->parentPageNo = rightPageId;
				} else {
					NonLeafNodeInt *child = (NonLeafNodeInt *)childNode;
					child->parentPageNo = rightPageId;
				}
				bufMgr->unPinPage(file, rightPage->pageNoArray[i], true);
			}

			// Insert the middle pair of <key, rid> to the parent node
			if(curNode->parentPageNo == std::uint32_t(-1)) {
				allocateNonLeafNode(curNode->parentPageNo);
				bufMgr->unPinPage(file, curNode->parentPageNo, true);
				rightPage->parentPageNo = curNode->parentPageNo;
				rootPageNum = curNode->parentPageNo;
			}

			PageId parentPageId = curNode->parentPageNo;
			bufMgr->unPinPage(file, nonLeafId, true);
			bufMgr->unPinPage(file, rightPageId, true);
			insertToNonLeaf(parentPageId, parentKey, nonLeafId, rightPageId, 0);
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
	// Situation 1: Empty Tree => Allocate a new leaf node (new root)
    if(leafOccupancy == 0) {
		LeafNodeInt *root = allocateLeafNode(rootPageNum);
		root->keyArray[0] = *((int*)key); // Key set to be integer
		root->ridArray[0] = rid;
		root->numValidKeys++;
        root->rightSibPageNo = -1; // No right sibling yet
		bufMgr->unPinPage(file, rootPageNum, true);
		leafOccupancy++;

	// Situation 2: Not empty tree => Insert to appropriate leaf node
	} else {
		// First locate the appropriate leaf node to insert
		PageId targetLeaf;
		searchForLeaf(targetLeaf, *((int*)key));
		insertToLeaf(targetLeaf, *((int*)key), rid);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    // Add your code below. Please do not remove this line.
	if(*((int *)lowValParm)> *((int *)highValParm)){
		throw BadScanrangeException();
	}
	if(lowOpParm != GT && lowOpParm != GTE){
		throw BadOpcodesException();
	}
	if(highOpParm != LT && highOpParm != LTE){
		throw BadOpcodesException();
	}
	lowValInt = *((int *)lowValParm);
	highValInt = *((int *)highValParm);
	lowOp = lowOpParm;
	highOp = highOpParm;
	scanExecuting = true;
	//get the leaf node for the low param
	searchForLeaf(currentPageNum,lowValInt);
	// std::cout << "Current Page Number is: " << currentPageNum << std::endl;
	bufMgr->readPage(file, currentPageNum, currentPageData);
	LeafNodeInt *cur = (LeafNodeInt *)currentPageData;
	nextEntry = 0;
	// coordinate to use the global instance nextEntry to track the record
	while(cur->keyArray[nextEntry] < lowValInt){
		if(nextEntry == cur->numValidKeys){
			if(cur->rightSibPageNo == std::uint32_t(-1)) { // Reached beyond the rightest node
				throw NoSuchKeyFoundException();
			}
			PageId lastPageNum = currentPageNum;
  			currentPageNum = cur->rightSibPageNo;
			bufMgr->unPinPage(file, lastPageNum, false);
  			bufMgr->readPage(file, currentPageNum, currentPageData);
  			nextEntry = 0;
			cur = (LeafNodeInt *)currentPageData;
		} else {
			nextEntry++;
		}
	}
	if(cur->keyArray[nextEntry] == lowValInt && lowOpParm == GT) {
		if(nextEntry < (cur->numValidKeys - 1)) {
			nextEntry++;
		} else { // Reached the rightest valid node of curNode
			// Go to the right sibling if there's one
			if(cur->rightSibPageNo == std::uint32_t(-1)) { // Reached beyond the rightest node
				throw NoSuchKeyFoundException();
			} else {
				nextEntry = 0;
				PageId lastPageNum = currentPageNum;
				currentPageNum = cur->rightSibPageNo;
				bufMgr->unPinPage(file, lastPageNum, false);
				bufMgr->readPage(file, currentPageNum, currentPageData);
			}
		}
	}
	if (cur->keyArray[nextEntry] > highValInt) {
    	throw NoSuchKeyFoundException();
  	}
	bufMgr->unPinPage(file, currentPageNum, false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}
	if(currentPageNum == std::uint32_t(-1)) {
		throw IndexScanCompletedException();
	} else {
		bufMgr->readPage(file, currentPageNum, currentPageData);
	}
	LeafNodeInt *cur = (LeafNodeInt *)currentPageData;
	int curVal = cur -> keyArray[nextEntry];
	if ( curVal > highValInt ||(curVal == highValInt && highOp == LT)) {
		throw IndexScanCompletedException();
	}
	outRid = cur -> ridArray[nextEntry];
	// std::cout << "nextKey = " << cur->keyArray[nextEntry] << std::endl;
	// std::cout << "nextEntry = " << nextEntry << std::endl;
	// std::cout << "numValidKey = " << cur->numValidKeys << std::endl;
	if(nextEntry < (cur->numValidKeys - 1)) {
		nextEntry++;
		bufMgr->unPinPage(file, currentPageNum, false);
	} else if(nextEntry == (cur->numValidKeys - 1)){
		// if(cur->rightSibPageNo == std::uint32_t(-1)) { // Reached beyond the rightest node
		// 	throw IndexScanCompletedException();
		// }
		PageId lastPageId = currentPageNum;
  		currentPageNum = cur->rightSibPageNo;
		bufMgr->unPinPage(file, lastPageId, false);
  		// bufMgr->readPage(file, currentPageNum, currentPageData);
  		nextEntry = 0;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}
	scanExecuting = false;
	if(currentPageNum != std::uint32_t(-1)) {
		bufMgr->unPinPage(file, currentPageNum, false);
	}
	return;
}

}

#ifndef RO_H
#define RO_H
#include "db.h"

// -1 to represent unused file slot in file pointer table
// and the whether the buffer slot is stored with pages we read from table
#define UNUSED -1

// cant use int32_t to store uint32_t number, therefore we use int64_t
#define INT64 int64_t

// create an page with usage count for buffer management
// each time we read one page into buffer slot
typedef struct Page{
    UINT64 pid;             // index of current page in the table,
                            // page index of each file always start from 0

    UINT oid;               // which table we read
    UINT nattrs;            // number of attributes from that table
    UINT ntuples_per_page;  // capacity for storing maximum tuples
    UINT ntuples;           // current number of tuples stored in page
    UINT pin_count;         // pin count for clock sweep replacement
    UINT usage;             // usage count for clock sweep replacement

    Tuple tuples[];
}Page;

typedef struct File{
    INT flag;               // -1(UNUSED) if the slot in file pointer table is not used, otherwise 1
    UINT oid;               // oid of the opened file 
    FILE* file_opened;      //  file descriptor
}File;

typedef struct Table_meta{
    UINT oid;
    UINT nattrs;
    UINT ntuples;
    INT ntuples_per_page;
    UINT64 npages;
}Table_meta;

void init();
void release();

// equality test for one attribute
// idx: index of the attribute for comparison, 0 <= idx < nattrs
// cond_val: the compared value
// table_name: table name
_Table* sel(const UINT idx, const INT cond_val, const char* table_name);

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name);
#endif
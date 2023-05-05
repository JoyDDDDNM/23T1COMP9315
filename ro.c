#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <string.h>
#include <stdbool.h>
#include <math.h>

// store pointers to each page we read from hard drive
struct Page **buffer_pool = NULL;

// next evict 
UINT nvb = 0;

// record the opened file, in which we use -1 to represent unused table slot
// and any positive number to represent an opened table(oid)
File* file_table = NULL;

// get page size for scanning each page
UINT page_size;

// number of slots in buffer pool
UINT nslots;

// maximum number of files we can open
UINT file_limit;

// for file pointer table update when table is full, we need to close
// one file to open an new file
UINT next_delete = 0;

// maximum number of tuples per page
INT max_ntuples_per_page;

// maximum attribute of the table with smallest tuple
UINT nattrs_max;

// release the page from buffer pool
static void clean_buffer(UINT i,INT flag){
   buffer_pool[i] -> oid = 0;
   buffer_pool[i] -> usage = 0;
   buffer_pool[i] -> pin_count = 0;
   buffer_pool[i] -> nattrs = 0;
   buffer_pool[i] -> ntuples_per_page = 0;
   buffer_pool[i] -> ntuples = 0;
   buffer_pool[i] -> pid = 0;

    // free each pointer in pointer arrays and make them point to NULL
    for (INT ntup = 0; ntup < max_ntuples_per_page; ntup++){
        // we are initializing buffer if flag == 0
        // we can't free pointer to tuple when we are initailizing them at the first time
        // because this pointer may points to somewhere which is not in heap memory
        if (flag == 0){
            buffer_pool[i] -> tuples[ntup] = NULL;
        }
        else if (flag == 1){
            // reset all value to 0
            for (UINT na = 0; na < nattrs_max; na++){
                buffer_pool[i] -> tuples[ntup][na] = 0;
            }
        }
        else if (flag == 2){
            // release all resources when all queries finished
            // free all tuples stored in page
            free(buffer_pool[i] -> tuples[ntup]);            
        }
    }

    // free page
    if (flag == 2){
        free(buffer_pool[i]);
    }
}

// check whether a file is opened in file pointer table
static bool is_file_open(UINT oid){
    for(int i = 0; i < file_limit; i++){
        if (file_table[i].oid == oid
            && file_table[i].flag != UNUSED){
            return true;
        }
    }
    return false;
}

// check whether the page we want is stored in buffer pool
static bool in_buffer_pool(UINT64 page_number,UINT oid){
    // traverse all slots to check whether the page is in buffer pool
    for (UINT i = 0; i < nslots; i++){
        if (buffer_pool[i] -> pid == page_number 
            &&  buffer_pool[i] -> oid == oid)
        {

            return true;
        }
    }
    return false;
}

// check whether file pointer table is full
static INT64 is_full(){
    for (INT64 i = 0; i < file_limit; i++){
        // the i-th slot in file pointer table is not used, return index to open an new file
        if (file_table[i].flag == UNUSED){
            return i;
        }
    }

    // file pointer table is full
    return -1;
}

// open an new file and store in file pointer table
// if table is full, close one of the file
static void open_file(UINT oid){
    // get data file path
    char path[] = "./data/";

    // the longest length for oid in char is 11 bytes, 2^32 = 4294967296
    char data_file[11] = "";
    sprintf(data_file,"%d",oid);
    strcat(path,data_file);

    // open the corresponding data file and start reading pages
    FILE* query_file = fopen(path,"r");

    INT64 file_index = is_full();
    // if file pointer table is not full
    if (file_index != -1){
        file_table[file_index].flag = 1;
        file_table[file_index].oid = oid;
        file_table[file_index].file_opened = query_file;
        log_open_file(oid);
    }
    // file pointer table is full, delete one of the file and open an new file
    else{
        // close previous file
        INT64 old_file_oid = file_table[next_delete%file_limit].oid;
        fclose(file_table[next_delete%file_limit].file_opened);
        log_close_file(old_file_oid);
        
        // open new file
        file_table[next_delete%file_limit].oid = oid;
        file_table[next_delete%file_limit].file_opened = query_file;
        log_open_file(oid);
        next_delete++;
    }
}

// get a free buffer using clock-sweep
static UINT get_free_buffer_slot(){
    // all slots are used, try to find an possible victim buffer
    while (true){
        if (buffer_pool[nvb] -> usage == 0 && buffer_pool[nvb] -> pin_count == 0){    
            // free the page in buffer pool, will be assigned for the page of new file later
            clean_buffer(nvb,1);

            if (buffer_pool[nvb] -> ntuples != 0){
                log_release_page(buffer_pool[nvb]->pid);
            }

            UINT ret = nvb;
            nvb = (nvb + 1) % nslots;
            return ret;
        }
        else{
            if ( buffer_pool[nvb] -> usage != 0){
                buffer_pool[nvb] -> usage--;
            }
            
            nvb = (nvb + 1) % nslots;
        }
    }   
}

// read page into buffer pool
static void read_into_buffer_pool(UINT free_buffer_slot_index,
                                  UINT64 pid,
                                  UINT oid,
                                  UINT ntuples_per_page,
                                  UINT nattrs,
                                  FILE* query_file,
                                  UINT64 npages){
    // start reading page into buffer pool
    log_read_page(pid);

    // update page meta data in buffer pool
    buffer_pool[free_buffer_slot_index] -> oid = oid;
    buffer_pool[free_buffer_slot_index] -> pid = pid;
    buffer_pool[free_buffer_slot_index] -> ntuples_per_page = ntuples_per_page;
    buffer_pool[free_buffer_slot_index] -> nattrs = nattrs;
    
    // find the page we want to read
    UINT64 current_pageId;
   
    UINT64 result_pos = 0;

    for (UINT64 pos = 0; pos < npages; pos++){
         // move to start of each page
        fseek(query_file,page_size*pos,SEEK_SET);
        fread(&current_pageId,sizeof(UINT64),1,query_file);
       
        // find the page id we want
        if (current_pageId == pid){
            result_pos = pos;
        }
    }
    // read one page
    // move to the current page from the beginning of file
    fseek(query_file, page_size*result_pos, SEEK_SET);

    // skip page id
    fseek(query_file, sizeof(UINT64), SEEK_CUR);

    // read each tuple in the page
    // record index of each tuple to store in buffer
    UINT tuple_index = 0;

    for (UINT tuple = 0; tuple < ntuples_per_page; tuple++){        
        // each time we scan one attribute in a tuple
        INT each_tuple[nattrs];

        fread(each_tuple,sizeof(INT),nattrs,query_file);

        // check whether we reach the end of each page
        bool reach_end = true;
        for (UINT attr = 0; attr < nattrs; attr++){
            if (each_tuple[attr] != 0){
                reach_end = false;
            }
        }

        // end of page
        if (reach_end){
            break;
        }

        // not the end of page, store this tuple into buffer
        for (UINT at = 0; at < nattrs; at++){
            buffer_pool[free_buffer_slot_index] -> tuples[tuple_index][at] = each_tuple[at];    
        }
        tuple_index++;
    }

    // record total number of tuples we store
    buffer_pool[free_buffer_slot_index] -> ntuples = tuple_index;
    
    // set the rest of pointers to points to array of 0
    while (tuple_index != max_ntuples_per_page){
        for (UINT i = 0; i < nattrs; i++){
            buffer_pool[free_buffer_slot_index] -> tuples[tuple_index][i] = 0;
        }

        tuple_index++;
    }
}

// get the index of page stored in buffer
static UINT get_page_index(UINT64 pid,UINT oid){
    UINT ret_index = 0;
    for (UINT i = 0; i < nslots; i++){
        if (buffer_pool[i] -> oid == oid
         && buffer_pool[i] -> pid == pid){
            ret_index = i;
         }
    }
    return ret_index;
}

// get the corresponding file descriptor to read page
static FILE* get_file_descriptor_from_table(UINT oid){
    FILE *ret_file = NULL;
    
    for (UINT i = 0; i < file_limit; i++){
        if (file_table[i].flag != UNUSED 
            && file_table[i].oid == oid) 
        {
            ret_file = file_table[i].file_opened;
        };
    }

    return ret_file;
};

// find an available index in the slot of hash table to insert an new tuple
static UINT find_next_free_tuple(Tuple** hash_table,UINT hash_index,UINT ntuples_per_page,UINT nattrs){
    // starting from the end of slot, find the first free tuple
    UINT ret = ntuples_per_page - 1;
    
    // check each tuple
    while (ret != -1){
        bool is_free = true;
        for (UINT at = 0; at < nattrs; at++){
            if (hash_table[hash_index][ret][at] != 0){
                is_free = false;
                break;
            }
        }
        
        // find an free slot to insert tuple
        if (is_free){
            break;
        }

        // go to check next slot
        ret--;
    }
    
    // if slot is full, return -1, else return index of an available tuple
    return ret;
}

// insert tuple into hash table
static void insert_tuple_into_slot(Tuple** hash_table,UINT hash_index,UINT slot_index,UINT page_index,UINT np){
    UINT nattrs = buffer_pool[page_index] -> nattrs;

    // assign each attribute
    for (UINT at = 0; at < nattrs; at++){
        hash_table[hash_index][slot_index][at] = buffer_pool[page_index] -> tuples[np][at];
    }
}

// clear one of the slot in hash table when it is full
static void clear_slot(Tuple** hash_table,UINT hash_index,UINT ntuples_per_page,UINT nattrs){
    // reset each attribute of each tuple to 0
    for (UINT np = 0; np < ntuples_per_page; np++){
        for (UINT at = 0; at < nattrs; at++){
            hash_table[hash_index][np][at] = 0;
        }
    }
}

static UINT get_requested_page(UINT64 pid,UINT oid,INT ntuples_per_page,UINT nattrs,UINT64 npages){
    UINT page_index = 0;

    // the page we query is not in buffer pool
    if (!in_buffer_pool(pid,oid)){
                    
        // implement clock-sweep replacement to get next free buffer
        page_index = get_free_buffer_slot();

        FILE *query_file = get_file_descriptor_from_table(oid);

        // read page from hard drive and store into buffer pool
        read_into_buffer_pool(page_index,
                                pid,
                                oid,
                                ntuples_per_page,
                                nattrs,
                                query_file,
                                npages);

    }

    // the page we query is in buffer pool
    else{
        page_index = get_page_index(pid,oid);
    }

    // increase pin count for current transcation
    buffer_pool[page_index] -> pin_count++;
    
    // increase the popularity of page
    buffer_pool[page_index] -> usage++;

    return page_index;
}

static Table_meta get_table_meta(const char* table1_name) {
    struct Table_meta table;
     
    Database* db = get_db();
    UINT ntables = db -> ntables;
    
    // get meta data of table 1 
    for (UINT i = 0; i < ntables; i++){
        char *each_table = db -> tables[i].name;
        if (strcmp(each_table,table1_name) == 0){
            table.oid = db -> tables[i].oid;
            table.nattrs = db -> tables[i].nattrs;
            table.ntuples = db -> tables[i].ntuples;
        }
    }

    // get number of pages for the table we open
    table.ntuples_per_page = (page_size-sizeof(UINT64))/sizeof(INT)/table.nattrs;
    // table.npages = ceil(table.ntuples/table.ntuples_per_page);
    UINT ntuples = table.ntuples; 
    INT ntuples_per_page = table.ntuples_per_page;
    table.npages = 0;
    while (ntuples > ntuples_per_page){
        table.npages++;
        ntuples -= ntuples_per_page;
    }

    table.npages++;

    return table;
}

// return all page id we read all the corresponding file
static void get_page_ids(UINT oid, UINT64 npages, UINT64 pageId_array[]){    
    
    // if the file is not opened, open(stored) in file pointer table
    if(!is_file_open(oid)){
        open_file(oid);
    };

    FILE *query_file = get_file_descriptor_from_table(oid);

    UINT64 current_index = 0;
    UINT64 current_pageId = 0;

    // read all page id store it into return array
    for (UINT64 i = 0; i < npages; i++){
        fseek(query_file,page_size*i,SEEK_SET);
        fread(&current_pageId,sizeof(UINT64),1,query_file);
        pageId_array[current_index] = current_pageId;
        current_index++;
    }
}

// initialize buffer pool and file pointer table
void init(){
    // config all meta data
    Conf* cf = get_conf();

    // initialize page size
    page_size = cf -> page_size;

    // first calculate the maximum number of tuples per page, then use it to initialize buffer pool
    Database *db = get_db();
    
    // find minimal number of attributes pei page in each table
    // maximum number of attributes is 2^32 - 1, which is 4,294,967,295
    UINT nattrs_min = 4294967295;

    nattrs_max = 0;

    for (UINT i = 0; i < db->ntables; i++){
        if (db->tables[i].nattrs < nattrs_min){
            nattrs_min = db->tables[i].nattrs;
        }
        if (db->tables[i].nattrs > nattrs_max){
            nattrs_max = db->tables[i].nattrs;
        }
    }

    // maximum number of tuples per page
    max_ntuples_per_page = (page_size-sizeof(UINT64))/sizeof(INT)/nattrs_min;

    // get number of buffer to initialize buffer_pool
    nslots = cf -> buf_slots;
    
    // initialize each pointer to page
    buffer_pool= malloc(sizeof(Page*) * nslots);

    // malloc space for each pointer to page
    for (UINT i = 0; i < nslots; i++){
        buffer_pool[i] = malloc(sizeof(Page) + (max_ntuples_per_page*sizeof(Tuple)));
    }

    // reset all member to page to default
    for (UINT i = 0; i < nslots; i++){
        clean_buffer(i,0); 
    }

    // initialize each tuple slot in each page
    for (UINT i = 0; i < nslots; i++){
        for (INT np = 0; np < max_ntuples_per_page; np++){
            Tuple each_tuple = malloc(sizeof(INT)*nattrs_max);
            for (int na = 0; na < nattrs_max; na++){
                each_tuple[na] = 0;
            }
            buffer_pool[i] -> tuples[np] = each_tuple;
        }
    }

    // get file limit to initialize file table
    file_limit = cf -> file_limit;

    // initialize file table, with each element initialized to -1(unused)
    file_table = malloc(sizeof(File) * file_limit);
    for (UINT i = 0; i < file_limit; i++){
        file_table[i].flag = UNUSED;
        file_table[i].oid = 0;
        file_table[i].file_opened = NULL;
    }

    printf("init() is invoked.\n");
}

// release buffer pool and file pointer table
void release(){

    // free each tuple and page stored in buffer
    for (UINT i = 0; i < nslots; i++){
       clean_buffer(i,2);
    }
    
    // free buffer
    free(buffer_pool);

    // close each opened file
    for (UINT i = 0; i < file_limit; i++){
        if (file_table[i].flag != UNUSED){
            fclose(file_table[i].file_opened);
        }
    }

    // free file pointer table
    free(file_table);
 
    printf("release() is invoked.\n");
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    printf("sel() is invoked.\n");

    Table_meta table = get_table_meta(table_name);

    // get meta data of table 1 
    UINT oid = table.oid;
    UINT nattrs  = table.nattrs;
    UINT ntuples = table.ntuples;
    INT ntuples_per_page = table.ntuples_per_page;
    UINT64 npages =  table.npages;

    // temporary tuple to store resulting tuples
    INT temp_table[ntuples][nattrs];
    for (UINT i = 0; i < ntuples; i++){
        for (UINT j = 0; j < nattrs; j++){
            temp_table[i][j] = 0;
        }
    }

    // number of tuples to return
    UINT ntuple_ret = 0;

    // read all page ids first
    UINT64 pageId_array[npages];
    get_page_ids(oid, npages,pageId_array);
    
    // for each page, first try to find it in buffer pool, if it doesn't
    // exist in buffer pool, check whether it is opened in file pointer table
    // is not, open it and store file pointer into file pointer table
    for(UINT64 pid_index = 0; pid_index < npages; pid_index++){
        // get current page id
        UINT64 pid = pageId_array[pid_index];

        UINT page_index = get_requested_page(pid,oid,ntuples_per_page,nattrs,npages);

        // increase the count of tuples by checking whether the corresponding index of tuple
        // contains the value we want
        for (UINT tuple_index = 0; tuple_index < buffer_pool[page_index] -> ntuples; tuple_index++){
            if (buffer_pool[page_index] -> tuples[tuple_index][idx] == cond_val){
                // store tuple containning correct value
                for (UINT j = 0; j < nattrs; j++){
                    temp_table[ntuple_ret][j] = buffer_pool[page_index] -> tuples[tuple_index][j];
                }
                ntuple_ret++;
            }
        }

        // release page, decrease pin count by 1
        if (buffer_pool[page_index] -> pin_count != 0){
            buffer_pool[page_index] -> pin_count--;
        }
    }
    // create return table
    _Table* ret_table =  malloc(sizeof(_Table)+ntuple_ret*sizeof(Tuple));

    ret_table -> nattrs = nattrs;
    ret_table -> ntuples = ntuple_ret;
    for (UINT i = 0; i < ntuple_ret; i++){
        Tuple t = malloc(sizeof(INT)*ret_table->nattrs);
        // copy each tuple from temporary tuples into return tuple
        for (UINT attr = 0; attr < nattrs; attr++){
            INT ret_attr = temp_table[i][attr];
            t[attr] = ret_attr;
        }
        ret_table -> tuples[i] = t;
    }

    return ret_table;
}

// block nested foor loop join
static _Table* nested_for_loop_join(UINT oid_1,
                                    UINT oid_2,
                                    UINT64 npages_1,
                                    UINT64 npages_2,
                                    UINT ntuples_per_page_1,
                                    UINT ntuples_per_page_2,
                                    UINT nattrs_1,
                                    UINT nattrs_2,
                                    UINT idx1,
                                    UINT idx2,
                                    UINT ntuples_1,
                                    UINT ntuples_2,
                                    INT flag){
    // read all page ids of table 1 first
    UINT64 pageId_array_1[npages_1];
    get_page_ids(oid_1, npages_1,pageId_array_1);
    
    // read all page ids of table 2
    UINT64 pageId_array_2[npages_2];
    get_page_ids(oid_2, npages_2,pageId_array_2);

    // maximum tuples we can get after join
    UINT64 max_tuples = ntuples_1*ntuples_2;

    // temporary tuple to store resulting tuples
    INT temp_table[max_tuples][nattrs_1 + nattrs_2];

    for (UINT i = 0; i < max_tuples; i++){
        for (UINT j = 0; j < nattrs_1 + nattrs_2; j++){
            temp_table[i][j] = 0;
        }
    }

    // number of tuples to return
    UINT ntuple_ret = 0;


    for (UINT64 pid_index_1 = 0; pid_index_1 < npages_1; pid_index_1++){
        // get current page id
        UINT64 pid_1 = pageId_array_1[pid_index_1];

        // request page from table 1, and increase pin count
        get_requested_page(pid_1,oid_1,ntuples_per_page_1,nattrs_1,npages_1);

        // haven't read n-1 pages from table 1 and haven't reach the end of table 1
        if ( (pid_index_1 % (nslots - 1)) != nslots - 2 ){
            continue;
        }

         // read n - 1 pages from table 1, start compare them with table 2
        for (UINT64 pid_index_2 = 0; pid_index_2 < npages_2; pid_index_2++){
            // get current page id
            UINT64 pid_2 = pageId_array_2[pid_index_2];

            UINT page_index_2 = get_requested_page(pid_2,oid_2,ntuples_per_page_2,nattrs_2,npages_2);

            // compare all tuples of current page from table 2
            for (UINT np_2 = 0; np_2 < buffer_pool[page_index_2] -> ntuples; np_2++){
                
                // treaverse buffer pool to find pages of table 1
                for (UINT table_1_index = 0; table_1_index < nslots; table_1_index++){
                    // find a page from table 1
                    if (buffer_pool[table_1_index] -> oid == oid_1){
                        for (UINT np_1 = 0; np_1 < buffer_pool[table_1_index] -> ntuples; np_1++){
                            
                            // find one matching tuple, add to result table
                            if (buffer_pool[table_1_index] -> tuples[np_1][idx1] == buffer_pool[page_index_2] -> tuples[np_2][idx2]){
                                
                                // store each attribute of tuple from table 1 and 2 into temp_table
                                UINT na = 0;

                                // store the attribute of table 1 first
                                if (flag == 0){
                                    for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){
                                        temp_table[ntuple_ret][na] = buffer_pool[table_1_index] -> tuples[np_1][na_1];
                                        na++;
                                    }
                                    for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                        temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                        na++;
                                    }
                                }

                                // store the attribute of table 2 first
                                if (flag == 1){
                                    for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                        temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                        na++;
                                    }
                                    for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){
                                        temp_table[ntuple_ret][na] = buffer_pool[table_1_index] -> tuples[np_1][na_1];
                                        na++;
                                    }
                                }

                                ntuple_ret += 1;
                            }
                        }
                    }
                }
            }   

            // we have compare one page of table 2 with table 1, release this page
            if (buffer_pool[page_index_2] -> pin_count != 0){
               buffer_pool[page_index_2] -> pin_count--;
            }
        }

        // release all pages of table 1
        for (UINT table_1_index = 0; table_1_index < nslots; table_1_index++){
            if (buffer_pool[table_1_index] -> pin_count != 0 && buffer_pool[table_1_index] -> oid == oid_1){
                buffer_pool[table_1_index] -> pin_count--;
            }
        }
    }


    bool check_uncompared = false;
    for (UINT i = 0; i < nslots; i++){
        // there are still some pages of table 1 left uncompared in buffer 
        if (buffer_pool[i] -> oid == oid_1 && buffer_pool[i] -> pin_count != 0){
            check_uncompared = true;
        }
    }
    
    if (check_uncompared){
        for (UINT64 pid_index_2 = 0; pid_index_2 < npages_2; pid_index_2++){
            // get current page id
            UINT64 pid_2 = pageId_array_2[pid_index_2];

            UINT page_index_2 = get_requested_page(pid_2,oid_2,ntuples_per_page_2,nattrs_2,npages_2);
            // compare all tuples of current page from table 2
            for (UINT np_2 = 0; np_2 < buffer_pool[page_index_2] -> ntuples; np_2++){
                
                // treaverse buffer pool to find pages of table 1
                for (UINT table_1_index = 0; table_1_index < nslots; table_1_index++){
                    // find a page from table 1
                    if (buffer_pool[table_1_index] -> oid == oid_1){
                        for (UINT np_1 = 0; np_1 < buffer_pool[table_1_index] -> ntuples; np_1++){
                            
                            // find one matching tuple, add to result table
                            if (buffer_pool[table_1_index] -> tuples[np_1][idx1] == buffer_pool[page_index_2] -> tuples[np_2][idx2] 
                                && buffer_pool[table_1_index] -> pin_count != 0)
                            {    
                                // store each attribute of tuple from table 1 and 2 into temp_table
                                UINT na = 0;

                                // store the attribute of table 1 first
                                if (flag == 0){
                                    for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){
                                        temp_table[ntuple_ret][na] = buffer_pool[table_1_index] -> tuples[np_1][na_1];
                                        na++;
                                    }
                                    for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                        temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                        na++;
                                    }
                                }

                                // store the attribute of table 2 first
                                if (flag == 1){
                                    for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                        temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                        na++;
                                    }
                                    for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){
                                        temp_table[ntuple_ret][na] = buffer_pool[table_1_index] -> tuples[np_1][na_1];
                                        na++;
                                    }
                                }

                                ntuple_ret += 1;
                            }
                        }
                    }
                }
            }

            // we have compared one page of table 2 with table 1, release this page
            if (buffer_pool[page_index_2] -> pin_count != 0){
                buffer_pool[page_index_2] -> pin_count--;
            }
        }

        // release all pages of table 1
        for (UINT table_1_index = 0; table_1_index < nslots; table_1_index++){
            if (buffer_pool[table_1_index] -> pin_count != 0 && buffer_pool[table_1_index] -> oid == oid_1){
                buffer_pool[table_1_index] -> pin_count--;
            }
        }
    }

    // create return table and copy each tuple from temp table to return table
    _Table* ret_table =  malloc(sizeof(_Table)+ntuple_ret*sizeof(Tuple));
    ret_table -> nattrs = nattrs_1+nattrs_2;
    ret_table -> ntuples = ntuple_ret;
    for (UINT i = 0; i < ntuple_ret; i++){
        Tuple t = malloc(sizeof(INT)*ret_table->nattrs);
        // copy each tuple from temporary tuples into return tuple
        for (UINT attr = 0; attr < nattrs_1+nattrs_2; attr++){
            INT ret_attr = temp_table[i][attr];
            t[attr] = ret_attr;
        }
        ret_table -> tuples[i] = t;
    }

    return ret_table;
}

// use simple hash join 
static _Table* hash_join(UINT oid_1, 
                        UINT oid_2,
                        UINT64 npages_1,
                        UINT64 npages_2,
                        UINT ntuples_per_page_1,
                        UINT ntuples_per_page_2,
                        UINT nattrs_1,
                        UINT nattrs_2,
                        UINT idx1,
                        UINT idx2,
                        UINT ntuples_1,
                        UINT ntuples_2,
                        INT flag){ 

    // read all page ids of table 1 first
    UINT64 pageId_array_1[npages_1];
    get_page_ids(oid_1, npages_1,pageId_array_1);

    UINT64 pageId_array_2[npages_2];
    get_page_ids(oid_2, npages_2,pageId_array_2);

    // use hash table outside of buffer pool, we only read page into buffer pool
    UINT ntable = nslots - 2;
    Tuple* hash_table[ntable];

    // allocate memory for each pointer in hash table
    for (UINT i = 0; i < ntable; i++){
        // each slot in hash table can hold ntuples_per_page_1 
        hash_table[i] = malloc(sizeof(Tuple) * ntuples_per_page_1);
        
        // initialize each tuple and set to 0
        for (UINT np = 0; np < ntuples_per_page_1; np++){
            hash_table[i][np] = malloc(sizeof(INT)*nattrs_1);
            for (UINT na = 0; na < nattrs_1; na++){
                hash_table[i][np][na] = 0;
            }
        }
    }

    // maximum tuples we can get after join
    UINT64 max_tuples = ntuples_1*ntuples_2;

    // temporary tuple to store resulting tuples
    INT temp_table[max_tuples][nattrs_1 + nattrs_2];

    for (UINT i = 0; i < max_tuples; i++){
        for (UINT j = 0; j < nattrs_1 + nattrs_2; j++){
            temp_table[i][j] = 0;
        }
    }

    // number of tuples to return
    UINT ntuple_ret = 0;

    // perform hash join, reach tuples of each page from table 1 into hash table
    for (UINT64 pid_index_1 = 0; pid_index_1 < npages_1; pid_index_1++){
        // get current page id
        UINT64 pid_1 = pageId_array_1[pid_index_1];

        // request page from table 1
        UINT page_index = get_requested_page(pid_1,oid_1,ntuples_per_page_1,nattrs_1,npages_1);
        
        // read each tuple from buffer into hash table
        for (UINT np = 0; np < buffer_pool[page_index] -> ntuples; np++){
            // we use modular operation to determine which slot 
            // of hash table to insert tuple
            UINT hash_index = (buffer_pool[page_index] -> tuples[np][idx1])%ntable;
            
            // check whether the slot is full, if not, return the 
            // available tuple index to insert tuple
            UINT slot_index = find_next_free_tuple(hash_table,hash_index,ntuples_per_page_1,nattrs_1);
            
            // current slot is full or we reach the last page
            if (slot_index == -1){
                
                // check each tuple from table 2
                for (UINT64 pid_index_2 = 0; pid_index_2 < npages_2; pid_index_2++){
                    // get current page id
                    UINT64 pid_2 = pageId_array_2[pid_index_2];

                    // request page from table 2
                    UINT page_index_2 = get_requested_page(pid_2,oid_2,ntuples_per_page_2,nattrs_2,npages_2);

                    // scan each tuple of table 2 to compare with table 1
                    for (UINT np_2 = 0; np_2 < buffer_pool[page_index_2] -> ntuples; np_2++){
                        UINT hash_index_2 = (buffer_pool[page_index_2] -> tuples[np_2][idx2])%ntable;
                        
                        // the tuple of table 2 is not in same slot as the tuple of table 1
                        if (hash_index != hash_index_2){
                            continue;
                        }

                        // find each matching tuple and store it into temp_table
                        for (UINT tp = 0; tp < ntuples_per_page_1; tp++){
                            if (hash_table[hash_index][tp][idx1] == buffer_pool[page_index_2] -> tuples[np_2][idx2]){
                                // store each attribute of tuple from table 1 and 2 into temp_table
                                UINT na = 0;
                                
                                // store the attribute of table 1 first, given that the size of table is smaller
                                if (flag == 0){
                                    for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){
                                        temp_table[ntuple_ret][na] = hash_table[hash_index][tp][na_1];
                                        na++;
                                    }
                                    for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                        temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                        na++;
                                    }
                                }

                                // store the attribute of table 2 first
                                if (flag == 1){
                                    for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                        temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                        na++;
                                    }
                                    for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){
                                        temp_table[ntuple_ret][na] = hash_table[hash_index][tp][na_1];
                                        na++;
                                    }
                                }

                                ntuple_ret += 1;
                            }
                        }
                    }

                    // clear all tuples of current slot, reset all attributes to 0
                    clear_slot(hash_table,hash_index,ntuples_per_page_1,nattrs_1);

                    // release page of table 2, decrease pin count by 1
                    if (buffer_pool[page_index_2] -> pin_count != 0){
                        buffer_pool[page_index_2] -> pin_count--;
                    }

                }
            }
            insert_tuple_into_slot(hash_table,hash_index,slot_index,page_index,np);
        }

         // release page, decrease pin count by 1
        if (buffer_pool[page_index] -> pin_count != 0){
            buffer_pool[page_index] -> pin_count--;
        }
    }


    // finally we need to compare each tuple of table 2 with table 1 again
    // because some tuples of table 1 are still stored in hash table
    bool check_uncompared = false;
    for (UINT i = 0; i < ntable; i++){
        for (UINT np = 0; np < ntuples_per_page_1; np++){
            for (UINT na = 0; na < nattrs_1; na++){
                if (hash_table[i][np][na] != 0){
                    check_uncompared = true;
                }
            }
        }
    }

    if (check_uncompared) {
        for (UINT64 s = 0; s < npages_2; s++){
            // get current page id
            UINT64 pid_2 = pageId_array_2[s];

            // page index of current page in buffer pool
            UINT page_index_2 = get_requested_page(pid_2,oid_2,ntuples_per_page_2,nattrs_2,npages_2);

            // scan each tuple of table 2 to compare with table 1
            for (UINT np_2 = 0; np_2 < buffer_pool[page_index_2] -> ntuples; np_2++){
                UINT hash_index = (buffer_pool[page_index_2] -> tuples[np_2][idx2])%ntable;
                
                // find each matching tuple and store it into temp_table
                for (UINT i = 0; i < ntuples_per_page_1; i++){

                    if (hash_table[hash_index][i][idx1] == buffer_pool[page_index_2] -> tuples[np_2][idx2]){
                        // store each attribute of tuple from table 1 and 2 into temp_table
                        UINT na = 0;
                        
                        // store the attribute of table 1 first, given that the size of table is smaller
                        if (flag == 0){
                            for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){
                                temp_table[ntuple_ret][na] = hash_table[hash_index][i][na_1];
                                na++;
                            }
                            for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                na++;
                            }
                        }

                        // store the attribute of table 2 first
                        if (flag == 1){
                            for (UINT na_2 = 0; na_2 < nattrs_2; na_2++){
                                temp_table[ntuple_ret][na] = buffer_pool[page_index_2] -> tuples[np_2][na_2];
                                na++;
                            }
                            for (UINT na_1 = 0; na_1 < nattrs_1; na_1++){

                                temp_table[ntuple_ret][na] = hash_table[hash_index][i][na_1];
                                na++;
                            }
                        }

                        ntuple_ret += 1;
                    }
                }
            }

            // release page, decrease pin count by 1
            if (buffer_pool[page_index_2] -> pin_count != 0){
                buffer_pool[page_index_2] -> pin_count--;
            }
        }
    }

    // create return table and copy each tuple from temp table to return table
    _Table* ret_table =  malloc(sizeof(_Table)+ntuple_ret*sizeof(Tuple));
    ret_table -> nattrs = nattrs_1+nattrs_2;
    ret_table -> ntuples = ntuple_ret;
    for (UINT i = 0; i < ntuple_ret; i++){
        Tuple t = malloc(sizeof(INT)*ret_table->nattrs);
        // copy each tuple from temporary tuples into return tuple
        for (UINT attr = 0; attr < nattrs_1+nattrs_2; attr++){
            INT ret_attr = temp_table[i][attr];
            t[attr] = ret_attr;
        }
        ret_table -> tuples[i] = t;
    }

    // free hash table
    for (UINT i = 0; i < ntable; i++){
        // free each tuple of i-th slot
        for (UINT np = 0; np < ntuples_per_page_1; np++){
            free(hash_table[i][np]);
        }
        // free i-th slot 
        free(hash_table[i]);
    }

    return ret_table;
}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){ 
    printf("join() is invoked.\n");
    
    Table_meta table_1 = get_table_meta(table1_name);

    // get meta data of table 1 
    UINT oid_1 = table_1.oid;
    UINT nattrs_1  = table_1.nattrs;
    UINT ntuples_1 = table_1.ntuples;
    INT ntuples_per_page_1 = table_1.ntuples_per_page;
    UINT64 npages_1 =  table_1.npages;
    /*==============================================================================*/

    Table_meta table_2 = get_table_meta(table2_name);

    // get meta data of table 1 
    UINT oid_2 = table_2.oid;
    UINT nattrs_2  = table_2.nattrs;
    UINT ntuples_2 = table_2.ntuples;
    INT ntuples_per_page_2 = table_2.ntuples_per_page;
    UINT64 npages_2 =  table_2.npages;
    
    // get number of buffer slots
    Conf* cf = get_conf();
    UINT buf_slots = cf->buf_slots;

    // compare and choose proper join method
    UINT64 total_pages = npages_1 + npages_2;
    if (buf_slots < total_pages){
        if (npages_1 < npages_2){
            // table 1 is used for outer relation
            return nested_for_loop_join(oid_1,oid_2,npages_1,npages_2,ntuples_per_page_1,ntuples_per_page_2,nattrs_1,nattrs_2,idx1,idx2,ntuples_1,ntuples_2,0);
        }
        else{
            // table 2 is used for outer relation
            return nested_for_loop_join(oid_2,oid_1,npages_2,npages_1,ntuples_per_page_2,ntuples_per_page_1,nattrs_2,nattrs_1,idx2,idx1,ntuples_2,ntuples_1,1);     
        }
    }
    else{
        if (npages_1 < npages_2){
            // store tuples of table 1 into hash table
            return hash_join(oid_1,oid_2,npages_1,npages_2,ntuples_per_page_1,ntuples_per_page_2,nattrs_1,nattrs_2,idx1,idx2,ntuples_1,ntuples_2,0);
        }
        else{
            // store tuples of table 2 into hash table
            return hash_join(oid_2,oid_1,npages_2,npages_1,ntuples_per_page_2,ntuples_per_page_1,nattrs_2,nattrs_1,idx2,idx1,ntuples_2,ntuples_1,1);     
        }
    }
}
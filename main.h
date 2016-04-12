#ifndef __MAIN_H__
#define __MAIN_H__

#include "inc.h"

#include <tcbdb.h>
#define MAX_SECTION_NUM        12
#define MAX_PACKET_LEN         1024
#define QUEUE_MSG_TYPE         23
#define MAX_USER_NUMS          1000000*6
#define OFFSET_USERID          10000000

typedef unsigned char   __u8;
typedef unsigned short  __u16;
typedef unsigned int    __u32;
typedef unsigned long   __u64;

#define true      1
#define false     0
#define YES       true
#define NO        false
#define TRUE      YES
#define FALSE     NO

typedef struct distribute_mgr_st *distribute_mgr_t;
typedef struct section_mgr_st    *section_mgr_t;
typedef struct user_mgr_st       *user_mgr_t;

typedef struct distribute_mgr_st
{
    Cfg          *config;
    char         log_file_name[MAX_FILE_NAME_LENGTH];
    int          log_file_level;

    int          distribute_threads;

    char         db_file_usr[MAX_FILE_NAME_LENGTH];
    char         db_file_usr0[MAX_FILE_NAME_LENGTH];
    char         db_file_usr1[MAX_FILE_NAME_LENGTH];
    char         db_file_usr2[MAX_FILE_NAME_LENGTH];
    char         db_file_usr3[MAX_FILE_NAME_LENGTH];
    int          dbcache_nonleaf;
    int          dbcache_leaf;
    int          dbcache_mappedmemory;
    int          db_bucket_nums;

    TCBDB        *db_usr;
    TCBDB        *db_usr_sec0;
    TCBDB        *db_usr_sec1;
    TCBDB        *db_usr_sec2;
    TCBDB        *db_usr_sec3;

    char         section_path[MAX_FILE_NAME_LENGTH];
    char         share_path[MAX_FILE_NAME_LENGTH];
    int          secid01;
    int          s01recvid;
    int          s01recv_queue_id;
    int          s01sendid;
    int          s01send_queue_id;

    int          secid02;
    int          s02recvid;
    int          s02recv_queue_id;
    int          s02sendid;
    int          s02send_queue_id;

    int          secid03;
    int          s03recvid;
    int          s03recv_queue_id;
    int          s03send_queue_id;
    int          s03sendid;

    int          secid04;
    int          s04recvid;
    int          s04recv_queue_id;
    int          s04sendid;
    int          s04send_queue_id;

    section_mgr_t section[MAX_SECTION_NUM];

}DISTRIBUTE_MGR_ST;

typedef struct user_info_st
{
    __u32 uid;
    __u8  create:1;
    __u8  update:1;
    __u8  iscorp:1;
    __u8  gender:1;
    __u8  reserved:4;

    __u8  idtype;
    __u8  id[20];
    __u8  name[128];
    __u8  isdn[20];

    __u8  vipid[30];
    __u8  node[10];
    __u8  county[6];
    __u64 last;

    __u64 sum[2]; // 1-sa 0-t

    __u64 balance;
    __u64 average;
}USER_INTO_ST;

typedef struct user_mgr_st
{
    __u32  max_index;
    __u32  user_cnt;

    struct user_info_st slots[MAX_USER_NUMS];
}USER_MGR_ST;

typedef struct section_mgr_st {
    __u8         section[6];
    __u8         section_share_path[MAX_FILE_NAME_LENGTH];
    __u32        section_memory_id;

    struct user_mgr_st      u;
}SECTION_MGR_ST;

#endif



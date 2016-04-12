#include "inc.h"
#include "main.h"
#include "ipcutils.h"

extern bool quitting;
extern distribute_mgr_t distribute_mgr;

void section_initial(section_mgr_t section_item)
{
    int i = 0;

    user_mgr_t    u = &section_item->u;

    info(0, "user init ...");
    for (i=0; i<MAX_USER_NUMS; i++){
        u->slots[i].uid    = i + OFFSET_USERID;
        u->slots[i].create = false;
        u->slots[i].update = false;
    }
    u->max_index = 0;
    info(0, "user init ... end");
    return;
}

void init_params()
{
    strcpy(distribute_mgr->log_file_name, "distribute.log" );
    distribute_mgr->log_file_level = 0;

    distribute_mgr->distribute_threads = 4;

    distribute_mgr->dbcache_nonleaf      = 1024;
    distribute_mgr->dbcache_leaf         = 2048;
    distribute_mgr->dbcache_mappedmemory = 104857600;
    distribute_mgr->db_bucket_nums       = 50000000;

    distribute_mgr->db_usr_sec0 = NULL;
    distribute_mgr->db_usr_sec1 = NULL;
    distribute_mgr->db_usr_sec2 = NULL;
    distribute_mgr->db_usr_sec3 = NULL;

    distribute_mgr->s01recv_queue_id = -1;
    distribute_mgr->s01send_queue_id = -1;

    distribute_mgr->s02recv_queue_id = -1;
    distribute_mgr->s02send_queue_id = -1;

    distribute_mgr->s03recv_queue_id = -1;
    distribute_mgr->s03send_queue_id = -1;

    distribute_mgr->s04recv_queue_id = -1;
    distribute_mgr->s04send_queue_id = -1;
}

int init_data(int reload)
{
    int i,pmemid,ret;
    key_t request_key;
    section_mgr_t section_item;

    for (i=0; i<distribute_mgr->distribute_threads; i++)
    {
        pmemid = 100+i;
        distribute_mgr->section[i] = NULL;
        request_key = get_key_by_filename(distribute_mgr->share_path, 1,pmemid);
        ret = check_table(request_key);
        if (ret < 0) {
            section_item = (section_mgr_t)create_table(request_key,sizeof(SECTION_MGR_ST));
            section_item->section_memory_id = pmemid;
            snprintf(section_item->section, 6, "%d",i);
            snprintf(section_item->section_share_path, MAX_FILE_NAME_LENGTH, "%s", distribute_mgr->share_path);
            section_initial(section_item);
        } else {
            section_item = (section_mgr_t) open_table(request_key);
            if(reload) section_initial(section_item);
        }
        distribute_mgr->section[i] = section_item;
    }
    
    if (distribute_mgr->s01recv_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s01recvid);
        distribute_mgr->s01recv_queue_id  = create_queue(request_key);

        if (distribute_mgr->s01recv_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s01 recv queue:%d",distribute_mgr->s01recv_queue_id);

    if (distribute_mgr->s01send_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s01sendid);
        distribute_mgr->s01send_queue_id  = create_queue(request_key);

        if (distribute_mgr->s01send_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s01 send queue:%d",distribute_mgr->s01send_queue_id);

    if (distribute_mgr->s02recv_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s02recvid);
        distribute_mgr->s02recv_queue_id  = create_queue(request_key);

        if (distribute_mgr->s02recv_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s02 recv queue:%d",distribute_mgr->s02recv_queue_id);

    if (distribute_mgr->s02send_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s02sendid);
        distribute_mgr->s02send_queue_id  = create_queue(request_key);

        if (distribute_mgr->s02send_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s02 send queue:%d",distribute_mgr->s02send_queue_id);

    if (distribute_mgr->s03recv_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s03recvid);
        distribute_mgr->s03recv_queue_id  = create_queue(request_key);

        if (distribute_mgr->s03recv_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s03 recv queue:%d",distribute_mgr->s03recv_queue_id);

    if (distribute_mgr->s03send_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s03sendid);
        distribute_mgr->s03send_queue_id  = create_queue(request_key);

        if (distribute_mgr->s03send_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s03 send queue:%d",distribute_mgr->s03send_queue_id);

    if (distribute_mgr->s04recv_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s04recvid);
        distribute_mgr->s04recv_queue_id  = create_queue(request_key);

        if (distribute_mgr->s04recv_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s04 recv queue:%d",distribute_mgr->s04recv_queue_id);

    if (distribute_mgr->s04send_queue_id < 0)
    {
        key_t request_key = get_key_by_filename(distribute_mgr->section_path,1,distribute_mgr->s04sendid);
        distribute_mgr->s04send_queue_id  = create_queue(request_key);

        if (distribute_mgr->s04send_queue_id < 0)
        {
            error(errno, "Creating driver in queue(%x) failed", request_key);
            return -1;
        }
    }

    info(0,"Create s04 send queue:%d",distribute_mgr->s04send_queue_id);

    distribute_mgr->db_usr = tcbdbnew();
    distribute_mgr->db_usr_sec0 = tcbdbnew();
    distribute_mgr->db_usr_sec1 = tcbdbnew();
    distribute_mgr->db_usr_sec2 = tcbdbnew();
    distribute_mgr->db_usr_sec3 = tcbdbnew();

    tcbdbtune(distribute_mgr->db_usr, 2048, 4096, distribute_mgr->db_bucket_nums, 8, 10, BDBTLARGE);
    tcbdbsetcache(distribute_mgr->db_usr, distribute_mgr->dbcache_leaf, distribute_mgr->dbcache_nonleaf);
    tcbdbsetxmsiz(distribute_mgr->db_usr, distribute_mgr->dbcache_mappedmemory);

    tcbdbtune(distribute_mgr->db_usr_sec0, 2048, 4096, distribute_mgr->db_bucket_nums, 8, 10, BDBTLARGE);
    tcbdbsetcache(distribute_mgr->db_usr_sec0, distribute_mgr->dbcache_leaf, distribute_mgr->dbcache_nonleaf);
    tcbdbsetxmsiz(distribute_mgr->db_usr_sec0, distribute_mgr->dbcache_mappedmemory);

    tcbdbtune(distribute_mgr->db_usr_sec1, 2048, 4096, distribute_mgr->db_bucket_nums, 8, 10, BDBTLARGE);
    tcbdbsetcache(distribute_mgr->db_usr_sec1, distribute_mgr->dbcache_leaf, distribute_mgr->dbcache_nonleaf);
    tcbdbsetxmsiz(distribute_mgr->db_usr_sec1, distribute_mgr->dbcache_mappedmemory);

    tcbdbtune(distribute_mgr->db_usr_sec2, 2048, 4096, distribute_mgr->db_bucket_nums, 8, 10, BDBTLARGE);
    tcbdbsetcache(distribute_mgr->db_usr_sec2, distribute_mgr->dbcache_leaf, distribute_mgr->dbcache_nonleaf);
    tcbdbsetxmsiz(distribute_mgr->db_usr_sec2, distribute_mgr->dbcache_mappedmemory);
 
    tcbdbtune(distribute_mgr->db_usr_sec3, 2048, 4096, distribute_mgr->db_bucket_nums, 8, 10, BDBTLARGE);
    tcbdbsetcache(distribute_mgr->db_usr_sec3, distribute_mgr->dbcache_leaf, distribute_mgr->dbcache_nonleaf);
    tcbdbsetxmsiz(distribute_mgr->db_usr_sec3, distribute_mgr->dbcache_mappedmemory);

    if(!tcbdbopen(distribute_mgr->db_usr, distribute_mgr->db_file_usr, BDBOWRITER|BDBOCREAT))
    {
        error(0, "Attention: Unable to open the usr database.");
        return -1;
    }

    if(!tcbdbopen(distribute_mgr->db_usr_sec0, distribute_mgr->db_file_usr0, BDBOWRITER|BDBOCREAT))
    {
        error(0, "Attention: Unable to open the usr0 database.");
        return -1;
    }

    if(!tcbdbopen(distribute_mgr->db_usr_sec1, distribute_mgr->db_file_usr1, BDBOWRITER|BDBOCREAT))
    {
        error(0, "Attention: Unable to open the usr1 database.");
        return -1;
    }

    if(!tcbdbopen(distribute_mgr->db_usr_sec2, distribute_mgr->db_file_usr2, BDBOWRITER|BDBOCREAT))
    {
        error(0, "Attention: Unable to open the usr2 database.");
        return -1;
    }
    
    if(!tcbdbopen(distribute_mgr->db_usr_sec3, distribute_mgr->db_file_usr3, BDBOWRITER|BDBOCREAT))
    {
        error(0, "Attention: Unable to open the usr3 database.");
        return -1;
    }

    return 0;
}

void get_general_cfg(Cfg *p)
{
    if (cfg_single_group(p, "general") < 0)
    {
        error(0,"No [general] config group,use default");
        return;
    }
    char param[128],value[1024];
    while (cfg_read(p, "%s%s", param, value) == 0)
    {
       debug(0,"Load general config:%s value:%s",param,value);

       if (strcmp(param, "log_file_name") == 0)
           strcpy(distribute_mgr->log_file_name , value );

       else if (strcmp(param, "log_file_level") == 0)
           distribute_mgr->log_file_level = atoi(value);

       else if (strcmp(param, "distribute_threads") == 0)
           distribute_mgr->distribute_threads = atoi(value);

       else if (strcmp(param, "share_path") == 0)
           strcpy(distribute_mgr->share_path,value);

       else if (strcmp(param, "db_file_usr") == 0)
           strcpy(distribute_mgr->db_file_usr,value);

       else if (strcmp(param, "db_file_usr0") == 0)
           strcpy(distribute_mgr->db_file_usr0,value);

       else if (strcmp(param, "db_file_usr1") == 0)
           strcpy(distribute_mgr->db_file_usr1,value);

       else if (strcmp(param, "db_file_usr2") == 0)
           strcpy(distribute_mgr->db_file_usr2,value);

       else if (strcmp(param, "db_file_usr3") == 0)
           strcpy(distribute_mgr->db_file_usr3,value);

       else if (strcmp(param, "dbcache_nonleaf") == 0)
           distribute_mgr->dbcache_nonleaf = atoi(value);

       else if (strcmp(param, "dbcache_leaf") == 0)
           distribute_mgr->dbcache_leaf = atoi(value);

       else if (strcmp(param, "dbcache_mappedmemory") == 0)
           distribute_mgr->dbcache_mappedmemory = atoi(value);

       else if (strcmp(param, "db_bucket_nums") == 0)
           distribute_mgr->db_bucket_nums = atoi(value);
    }
    info(0,"Load general configure success!");

    if (cfg_single_group(p, "Queue") < 0)
    {
        error(0,"No [Queue] config group,use default");
    }
    while (cfg_read(p, "%s%s", param, value) == 0)
    {
       debug(0,"Load Queue config:%s value:%s",param,value);

       if(strcmp(param, "section_path") == 0)
           strcpy(distribute_mgr->section_path,value);

       else if(strcmp(param, "secid01") == 0)
           distribute_mgr->secid01 = atoi(value);

       else if(strcmp(param, "s01recvid") == 0)
           distribute_mgr->s01recvid = atoi(value);

       else if(strcmp(param, "s01sendid") == 0)
           distribute_mgr->s01sendid = atoi(value);

       else if(strcmp(param, "secid02") == 0)
           distribute_mgr->secid02 = atoi(value);

       else if(strcmp(param, "s02recvid") == 0)
           distribute_mgr->s02recvid = atoi(value);

       else if(strcmp(param, "s02sendid") == 0)
           distribute_mgr->s02sendid = atoi(value);

       else if(strcmp(param, "secid03") == 0)
           distribute_mgr->secid03 = atoi(value);

       else if(strcmp(param, "s03recvid") == 0)
           distribute_mgr->s03recvid = atoi(value);

       else if(strcmp(param, "s03sendid") == 0)
           distribute_mgr->s03sendid = atoi(value);

       else if(strcmp(param, "secid04") == 0)
           distribute_mgr->secid04 = atoi(value);

       else if(strcmp(param, "s04recvid") == 0)
           distribute_mgr->s04recvid = atoi(value);

       else if(strcmp(param, "s04sendid") == 0)
           distribute_mgr->s04sendid = atoi(value);
    }
    info(0,"section_path %s",distribute_mgr->section_path);
    info(0,"Load Queue configure success!");
}

#define VERSION_NUM "1.0. 20160325"
#define VERSION_DATE __DATE__
#define chartonumber(x) (x-'0')

#include "main.h"

extern int queue_is_full(int msgid, size_t size);
extern void init_params();
int init_data(int reload);
void get_general_cfg(Cfg *p);

bool quitting = false;
distribute_mgr_t distribute_mgr;

char *trans_15_to_18(char *s, char *t_out)
{
    const char weight[] = {7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2};
    const char check[] = {'1','0','X','9','8','7','6','5','4','3','2'};

    int i, sum, mod;
    if (s == NULL)  return NULL;
    strncpy(t_out, s, 6);
    strcat(t_out, "19");
    strncat(t_out, s+6, 9);
    sum = 0;
    for (i=0; i<17; i++)
    {
        sum += (t_out[i]-48)*weight[i];
    }

    mod = sum%11;

    t_out[17] = check[mod];
    t_out[18] = '\0';

    return t_out;
}

int get_uid(char idtype,const char *id,const char *orgno,TCBDB *db_usr_sec, int argv)
{
    char *value_tmp;
    char value_input[32],county[6];
    char key[64];
    int uid;

    user_mgr_t u = &distribute_mgr->section[argv]->u;
    sprintf(key, "U:%C:%s:%s", idtype, id, orgno );
    value_tmp = tcbdbget2(db_usr_sec, key);
    if(value_tmp) {
        uid = atoi(value_tmp);
        free(value_tmp);
        if ((uid - OFFSET_USERID) > u->max_index)
            u->max_index = uid - OFFSET_USERID;
    }else{
        u->max_index++;
        uid = u->max_index + OFFSET_USERID;
        sprintf(value_input, "%u", uid);
        tcbdbput2(db_usr_sec, key,value_input);
    }
    return uid;
}

static void safe_closedb()
{
    tcbdbsync(distribute_mgr->db_usr);
    tcbdbsync(distribute_mgr->db_usr_sec0);
    tcbdbsync(distribute_mgr->db_usr_sec1);
    tcbdbsync(distribute_mgr->db_usr_sec2);
    tcbdbsync(distribute_mgr->db_usr_sec3);

    tcbdbclose(distribute_mgr->db_usr);
    tcbdbclose(distribute_mgr->db_usr_sec0);
    tcbdbclose(distribute_mgr->db_usr_sec1);
    tcbdbclose(distribute_mgr->db_usr_sec2);
    tcbdbclose(distribute_mgr->db_usr_sec3);

    tcbdbdel(distribute_mgr->db_usr);
    tcbdbdel(distribute_mgr->db_usr_sec0);
    tcbdbdel(distribute_mgr->db_usr_sec1);
    tcbdbdel(distribute_mgr->db_usr_sec2);
    tcbdbdel(distribute_mgr->db_usr_sec3);
    info(0,"close db end");

}

int distribute( int sec, char *data_in, msg_queue_t msg_data_in )
{
    int tmp_in,len_out,ret;

    tmp_in = strlen(data_in);
    memcpy( &msg_data_in->data[len_out], data_in, tmp_in );
    len_out = tmp_in;
    msg_data_in->msg_type = QUEUE_MSG_TYPE;
    if ( sec == 0 ){
        if(write_data_to_queue(distribute_mgr->s01send_queue_id,msg_data_in,len_out) < 0)
        {
            debug(0,"[%d]Emsgsnd1 Overflow[%s:%d]", msg_data_in->msg_type, __func__,__LINE__);
        }
    } else if( sec == 1 ){
        if(write_data_to_queue(distribute_mgr->s02send_queue_id,msg_data_in,len_out) < 0)
        {
            debug(0,"[%d]Emsgsnd2 Overflow[%s:%d]", msg_data_in->msg_type, __func__,__LINE__);
        }
    } else if( sec == 2 ){
        if(write_data_to_queue(distribute_mgr->s03send_queue_id,msg_data_in,len_out) < 0)
        {
            debug(0,"[%d]Emsgsnd3 Overflow[%s:%d]", msg_data_in->msg_type, __func__,__LINE__);
        }
    } else if( sec == 3 ){
        if(write_data_to_queue(distribute_mgr->s04send_queue_id,msg_data_in,len_out) < 0)
        {
            debug(0,"[%d]Emsgsnd4 Overflow[%s:%d]", msg_data_in->msg_type, __func__,__LINE__);
        }
    }
    return 0;
}

int wait_recv_data( int argv )
{
    char *ptr,*p[16];
    int msg_id,i,count,len_in,uid;
    struct msg_queue_st msg_data_in;
    TCBDB *db_usr_sec;
    
    if( argv == 0 ){
        msg_id = distribute_mgr->s01send_queue_id;
        db_usr_sec = distribute_mgr->db_usr_sec0;
    } else if( argv == 1 ){
        msg_id = distribute_mgr->s02send_queue_id;
        db_usr_sec = distribute_mgr->db_usr_sec1;
    } else if( argv == 2 ){
        msg_id = distribute_mgr->s03send_queue_id;
        db_usr_sec = distribute_mgr->db_usr_sec2;
    } else if( argv == 3 ){
        msg_id = distribute_mgr->s04send_queue_id;
        db_usr_sec = distribute_mgr->db_usr_sec3;
    } else {
        error( 0," no such message id exit... " );
        return -1;
    }
    count = 0;
    user_mgr_t u = &distribute_mgr->section[argv]->u;

    while (!quitting)
    {
        memset(&msg_data_in,0,sizeof(msg_data_in));

        len_in = msgrcv(msg_id,&msg_data_in, MAX_PKG_LEN,QUEUE_MSG_TYPE,0);
        if( len_in < 0 )
        {
           error(0,"Error msgrcv[%d][%s:%s:%d]",errno, __FILE__,__func__,__LINE__);
           sleep(1000);
           continue;
        }
        count++;

        unsigned char *data  = msg_data_in.data;
        ptr = data;
        i = 0;
        while (*ptr != 0x00 && i<4) {
            p[i++] = ptr;
            while (*ptr != 0x00 && *ptr != 0x2C)  ptr++;
            if (0x2C == *ptr) {
                *ptr = 0x00;
                ptr++;
            }
        }
        uid = get_uid(p[0][0],p[1],p[2],db_usr_sec,argv);
        uindex = uid - OFFSET_USERID;
        u->slots[uindex].uid = uid;
        u->slots[uindex].idtype = p[0][0];
        strcpy(u->slots[uindex].idno,p[1]);
        strcpy(u->slots[uindex].code,p[2]);
        if(count % 100000 == 0){
            info(0,"thread %d processing line %d",argv,count);
        }
    }
    return 0;
}

int main(int argc, char *argv[])
{
    char fullname[128];
    int argn = 1;
    FILE *fp = NULL;

    log_create(DEBUG);
    distribute_mgr = (distribute_mgr_t)malloc(sizeof(DISTRIBUTE_MGR_ST));
    memset(distribute_mgr,0,sizeof(DISTRIBUTE_MGR_ST));

    while ( argn < argc && argv[argn][0] == '-' && argv[argn][1] != '\0' )
    {
        if(strcmp(argv[argn],"-v") == 0 )
        {
            info(0,"Ver:%s Date:%s",VERSION_NUM,VERSION_DATE);
        }else if(strcmp(argv[argn],"-f") == 0 && argn + 1 < argc )
        {
            distribute_mgr->config = cfg_create("%s/cfg/%s", getenv("HOME"), argv[++argn]);
        }else if(strcmp(argv[argn],"-avg") == 0 && argn + 1 < argc )
        {
            strcpy(fullname, argv[++argn]);
        }
        argn++;
    }

    if(distribute_mgr->config == NULL){
        distribute_mgr->config = cfg_create("%s/cfg/distribute.conf", getenv("HOME"));
    }
    
    info(0,"%s(%s) startup... ...",argv[0], VERSION_NUM);

    init_params();
    get_general_cfg(distribute_mgr->config);
    log_append(distribute_mgr->log_file_name, distribute_mgr->log_file_level);
    log_set_level(distribute_mgr->log_file_level);
    init_data(0);
    
    if ((fp = fopen(fullname, "r")) == NULL) {
        error(0, "File[%s] open error!", fullname);
        goto exit_tab;
    }else{
        info(0,"Opened file[%s] to process... ...", fullname);
    }

    char idtype,idno[20];
    char data_in[MAX_PACKET_LEN],nid[20];
    char buffer[MAX_PACKET_LEN],fdate[9];
    char *ptr,*p[16];
    int  i,iscorp,dindex,count,skipline,numofid = -1;
    struct msg_queue_st msg_data_in;
    
    for(i=0;i<distribute_mgr->distribute_threads;i++){
        thread_create(wait_recv_data, i);
    }
    count = 0;
    while (!feof(fp)) {
        if (fgets(buffer, MAX_PACKET_LEN, fp) != NULL) {
            if (strstr(buffer, "#")) continue;

            count++;
            //证件类型,证件号码,用户姓名,用户手机号,用户账号,账户类型,币别,存期类型,余额,利率,数据日期,开户机构,开户日期
            //A,370788300000000000,,,6226171800000000,SA,,0,+500000.00,0,20130806,903066012 ,20110808
            i = 0;
            ptr = buffer;

            while (*ptr != 0x00 && i<14) {
                p[i++] = ptr;
                while (*ptr != 0x00 && *ptr != 0x2C)  ptr++;
                if (0x2C == *ptr) {
                    *ptr = 0x00;
                    ptr++;
                }
            }
            if (0x0A == *--ptr)  *ptr = 0x00;

            if (i != 13) {
                error(0, "Account file format error[line:%d], i = %d", count, i);
                continue;
            }

            if (!p[0][0] || !p[1][0] || !p[4][0] || !p[5][0] || !p[7][0] || !p[8][0] || !p[10][0] || !p[11][0]) {
                error(0, "Account file format error[line:%d], NULL check", count);
                continue;
            }
            if ('A' == p[0][0] && 15 == strlen(p[1])) {
                memset(nid, 0, 20);
                p[1] = trans_15_to_18(p[1], nid);
            }
            idtype = p[0][0];
            memcpy(idno, p[1], 20);
            numofid = chartonumber(p[1][17]);
            sprintf(data_in, "%C,%s,%s",idtype,idno,p[11] );
            if( numofid < 3 && numofid >= 0)
            {
                distribute(0,data_in,&msg_data_in);
            } else if( numofid < 5 && numofid >= 3)
            {
                distribute(1,data_in,&msg_data_in);
            } else if( numofid <= 7 && numofid >= 5)
            {
                distribute(2,data_in,&msg_data_in);
            } else if( numofid <= 9 && numofid >= 8)
            {
                distribute(3,data_in,&msg_data_in);
            } else {
                error(0,"illegal idno");
                return -1;
            }
        }
    } 
    //thread_join_all();
    
exit_tab:
    safe_closedb();
    if(fp) fclose(fp);
    cfg_destroy(distribute_mgr->config);

    info(0,"shutdown complete.");

    log_destroy();
    free(distribute_mgr);
    return 0;
    
}

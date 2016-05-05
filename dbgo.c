#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "dbgo.h"

#define MAX_BUFSIZ (8 * 1024)

typedef struct{
	time_t ts;
	off_t offset;
} db_entry_t;

typedef struct{
	DB *db;
	HASHINFO ih;
	BTREEINFO ib;
	void *info;
} dbop_t;

static int dbtype(const char *s) {
	if (!strcmp(s, "btree"))
		return (DB_BTREE);
	if (!strcmp(s, "hash"))
		return (DB_HASH);
	return -1;
	/* NOTREACHED */
}

static void * setinfo(dbop_t *dbop, int type, char *s){
	char *eq, *index();

	if ((eq = index(s, '=')) == NULL)
		return NULL;
	*eq++ = '\0';
	if (!isdigit(*eq))
		return NULL;

	switch (type) {
		case DB_BTREE:
			if (!strcmp("flags", s)) {
				dbop->ib.flags = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("cachesize", s)) {
				dbop->ib.cachesize = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("maxkeypage", s)) {
				dbop->ib.maxkeypage = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("minkeypage", s)) {
				dbop->ib.minkeypage = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("lorder", s)) {
				dbop->ib.lorder = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("psize", s)) {
				dbop->ib.psize = atoi(eq);
				return &dbop->ib;
			}
			break;
		case DB_HASH:
			if (!strcmp("bsize", s)) {
				dbop->ih.bsize = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("ffactor", s)) {
				dbop->ih.ffactor = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("nelem", s)) {
				dbop->ih.nelem = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("cachesize", s)) {
				dbop->ih.cachesize = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("lorder", s)) {
				dbop->ih.lorder = atoi(eq);
				return &dbop->ih;
			}
			break;
	}
	return NULL;
	/* NOTREACHED */
}

void * db_open(const char *fname, const char *typ, const char *inf, 
		int lock){
	dbop_t *dbop = NULL;
	char *infoarg, *p = NULL;
	int type;
	int oflags = O_CREAT | O_RDWR;

	dbop = calloc(sizeof(dbop), 1);
	if(dbop == NULL)
		return NULL;

	type = dbtype(typ);
	if(type < 0){
		goto err_out;
	}
	if(inf == NULL){
		infoarg = NULL;
	}else{
		infoarg = strdup(inf);
		if(infoarg == NULL)
			goto err_out;
		for (p = strtok(infoarg, ",\t "); p != NULL; 
				p = strtok(0, ",\t ")){
			if (*p != '\0'){
				dbop->info = setinfo(dbop, type, p);
			}
		}
		free(infoarg);
	}

	if(lock)
		oflags |= DB_LOCK;

	if ((dbop->db = dbopen(fname, oflags, S_IRUSR | S_IWUSR, 
					type, dbop->info)) == NULL){
		printf("dbopen faild\n");
		goto err_out;
	}

	return dbop;

err_out:
	if(dbop)
		free(dbop);
	return NULL;
}

int db_close(void *d) {
	return ((dbop_t *)d)->db->close(((dbop_t *)d)->db);
}

int db_get(void *d, const char *name, time_t *ts, off_t *offset, unsigned int flags){
	DBT key, data;
	int ret;
	DB *db;

	db = ((dbop_t *)d)->db;
	key.data = (char *)name;
	key.size = strlen(name)+1;

	ret = db->get(db, &key, &data, flags);
	if(ret){
		return -1;
	}else{
		*ts = ((db_entry_t *)(data.data))->ts;
		*offset = ((db_entry_t *)(data.data))->offset;
		return 0;
	}
}

int db_put(void *d, const char *name, time_t ts, off_t offset, unsigned int flags){
	DBT key, dat;
	int len;
	db_entry_t e;
	DB *db;

	db = ((dbop_t *)d)->db;
	if (!(len = strlen(name)))
		return -1;
	if (len > MAXKEYLEN)
		return -1;
	key.data = (char *)name;
	key.size = len+1;
	e.ts = ts;
	e.offset = offset;
	dat.data = &e;
	dat.size = sizeof(e);

	return db->put(db, &key, &dat, flags);
}

int db_delete(void *d, const char *name, unsigned int flags) {
	DBT key;
	DB *db;

	db = ((dbop_t *)d)->db;

	key.data = (char *)name;
	key.size = strlen(name)+1;
	return db->del(db, &key, flags);
}


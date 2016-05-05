#include "db.h"

#define MAXKEYLEN 1024
void * db_open(const char *fname, const char *typ, const char *inf, 
		int lock);
int db_close(void *db);
int db_get(void *db, const char *name, time_t *ts, 
		off_t *offset, unsigned int flags);
int db_put(void *db, const char *name, time_t ts, 
		off_t offset, unsigned int flags);
int db_delete(void *db, const char *name, unsigned int flags);

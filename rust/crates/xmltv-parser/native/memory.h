/* Private static-libxml2 allocation budget. Install before its first API call. */
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <libxml/xmlmemory.h>
typedef struct {
 _Atomic size_t used,peak,refs;
 _Atomic int failed;
 size_t limit;
} heap_budget;
typedef union {
 struct { size_t charge;heap_budget *owner; } value;
 max_align_t alignment;
} heap_header;
static _Thread_local heap_budget *current_heap;
static pthread_once_t memory_once=PTHREAD_ONCE_INIT;
static void heap_release(heap_budget *b){if(b&&atomic_fetch_sub(&b->refs,1)==1)free(b);}
static int heap_charge(heap_budget *b,size_t bytes){
 if(!b)return 1;
 size_t old=atomic_load(&b->used);
 for(;;){
  if(bytes>b->limit||old>b->limit-bytes){atomic_store(&b->failed,1);return 0;}
  if(atomic_compare_exchange_weak(&b->used,&old,old+bytes))break;
 }
 size_t peak=atomic_load(&b->peak);
 while(peak<old+bytes&&!atomic_compare_exchange_weak(&b->peak,&peak,old+bytes)){}
 return 1;
}
static void *heap_malloc(size_t bytes){
 if(bytes>SIZE_MAX-sizeof(heap_header))return NULL;
 size_t charge=bytes+sizeof(heap_header);heap_budget *b=current_heap;
 if(!heap_charge(b,charge))return NULL;
 heap_header *h=malloc(charge);
 if(!h){if(b){atomic_fetch_sub(&b->used,charge);atomic_store(&b->failed,1);}return NULL;}
 h->value.charge=charge;h->value.owner=b;if(b)atomic_fetch_add(&b->refs,1);
 return h+1;
}
static void heap_free(void *ptr){
 if(!ptr)return;
 heap_header *h=((heap_header*)ptr)-1;heap_budget *b=h->value.owner;
 if(b)atomic_fetch_sub(&b->used,h->value.charge);
 free(h);heap_release(b);
}
static void *heap_realloc(void *ptr,size_t bytes){
 if(!ptr)return heap_malloc(bytes);
 heap_header *old=((heap_header*)ptr)-1;heap_budget *b=old->value.owner;
 heap_budget *previous=current_heap;if(b)current_heap=b;
 /* Charge both live buffers while copying, including transient growth. */
 void *next=heap_malloc(bytes);current_heap=previous;if(!next)return NULL;
 size_t before=old->value.charge-sizeof(heap_header);
 memcpy(next,ptr,bytes<before?bytes:before);heap_free(ptr);return next;
}
static char *heap_strdup(const char *text){
 size_t len=strlen(text);char *out=heap_malloc(len+1);if(out)memcpy(out,text,len+1);return out;
}
static void memory_initialize(void){
 if(xmlMemSetup(heap_free,heap_malloc,heap_realloc,heap_strdup)!=0)abort();
 xmlInitParser();
}
static heap_budget *heap_new(size_t limit){
 pthread_once(&memory_once,memory_initialize);heap_budget *b=calloc(1,sizeof(*b));
 if(b){atomic_init(&b->used,0);atomic_init(&b->peak,0);atomic_init(&b->refs,1);atomic_init(&b->failed,0);b->limit=limit;}
 return b;
}

/* Isolated strict-bridge sanitizer/resource probe, not a production entrypoint.
 * Original Dispatcharr authors retain all application/algorithm credit. */
#include <stdio.h>
#include "shim.c"
typedef struct {dxml_parser *parser;size_t roots,nodes,text_bytes;} probe;
static void touch(probe *s,xmlNode *node){
 s->nodes++;char *text=dxml_text(node);if(text){s->text_bytes+=strlen(text);dxml_free(text);}
 for(xmlAttr *a=dxml_attr(node);a;a=dxml_attr_next(a)){char *value=dxml_attr_value(node,a);if(value){s->text_bytes+=strlen(value);dxml_free(value);}}
 for(xmlNode *child=dxml_child(node);child;child=dxml_next(child))touch(s,child);
}
static int collect(void *opaque,xmlNode *root){probe *s=opaque;touch(s,root);if(dxml_failed(s->parser))return 1;s->roots++;return 0;}
static size_t rss(void){FILE *f=fopen("/proc/self/status","r");if(!f)return 0;char line[256];size_t value=0;while(fgets(line,sizeof(line),f)){if(sscanf(line,"VmHWM: %zu kB",&value)==1)break;}fclose(f);return value;}
int main(int argc,char **argv){
 if(argc<2)return 64;FILE *input=fopen(argv[1],"rb");if(!input)return 65;size_t limit=argc>2?(size_t)strtoull(argv[2],NULL,10):16*1024*1024;
 probe state={0};state.parser=dxml_strict_new(collect,&state,limit);if(!state.parser){fclose(input);printf("{\"status\":2,\"roots\":0,\"allocation\":true}\n");return 0;}
 unsigned char buffer[32768];int status=0;size_t count;do{count=fread(buffer,1,sizeof(buffer),input);status=dxml_strict_feed(state.parser,(const char*)buffer,(int)count,count==0);if(status)break;}while(count);
 size_t peak=atomic_load(&state.parser->heap->peak);dxml_drop(state.parser);fclose(input);
 printf("{\"status\":%d,\"roots\":%zu,\"nodes\":%zu,\"text_bytes\":%zu,\"native_peak_bytes\":%zu,\"peak_rss_kib\":%zu}\n",status,state.roots,state.nodes,state.text_bytes,peak,rss());return 0;
}

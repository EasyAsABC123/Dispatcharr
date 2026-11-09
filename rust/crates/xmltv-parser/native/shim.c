/* Bounded XMLTV SAX2 bridge; original application credit: Dispatcharr authors. */
#include <libxml/parser.h>
#include <libxml/SAX2.h>
#include <libxml/tree.h>
#include <libxml/xmlerror.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "memory.h"
typedef int (*emit_fn)(void *,xmlNode *);
typedef struct { xmlParserCtxt *parser; emit_fn emit; void *opaque; heap_budget *heap; size_t bytes; size_t nodes; size_t dtd_input; int seen_root; int capacity; } dxml_parser;
static void quiet(void *ctx,const char *msg,...){(void)ctx;(void)msg;}
static void structured(void *ctx,const xmlError *error){(void)ctx;(void)error;}
static int charge(dxml_parser *s,size_t bytes,size_t nodes){
 s->bytes+=bytes;s->nodes+=nodes;
 if(s->bytes>4*1024*1024 || s->nodes>4096){s->capacity=1;xmlStopParser(s->parser);return 0;}
 return 1;
}
static void release_node(dxml_parser *s,xmlNode *node){(void)s;xmlUnlinkNode(node);xmlFreeNode(node);}
static void start(void *ctx,const xmlChar *local,const xmlChar *prefix,const xmlChar *uri,int nns,const xmlChar **ns,int nattrs,int ndefault,const xmlChar **attrs){
 xmlParserCtxt *parser=ctx;dxml_parser *s=parser->_private;s->seen_root=1;
 size_t bytes=xmlStrlen(local);
 for(int i=0;i<nattrs;i++)bytes+=xmlStrlen(attrs[5*i])+(size_t)(attrs[5*i+4]-attrs[5*i+3]);
 for(int i=0;i<nns;i++)bytes+=xmlStrlen(ns[2*i])+xmlStrlen(ns[2*i+1]);
 size_t nodes=1+(size_t)nattrs+(size_t)nns;
 if(!charge(s,bytes,nodes))return;
 xmlSAX2StartElementNs(ctx,local,prefix,uri,nns,ns,nattrs,ndefault,attrs);
}
static void chars(void *ctx,const xmlChar *data,int len){
 dxml_parser *s=((xmlParserCtxt*)ctx)->_private;if(charge(s,(size_t)len,0))xmlSAX2Characters(ctx,data,len);
}
static void cdata(void *ctx,const xmlChar *data,int len){
 dxml_parser *s=((xmlParserCtxt*)ctx)->_private;if(charge(s,(size_t)len,0))xmlSAX2CDataBlock(ctx,data,len);
}
static void comment(void *ctx,const xmlChar *data){
 dxml_parser *s=((xmlParserCtxt*)ctx)->_private;size_t bytes=xmlStrlen(data);
 if(charge(s,bytes,1))xmlSAX2Comment(ctx,data);
}
static void instruction(void *ctx,const xmlChar *target,const xmlChar *data){
 dxml_parser *s=((xmlParserCtxt*)ctx)->_private;size_t bytes=xmlStrlen(target)+xmlStrlen(data);
 if(charge(s,bytes,1))xmlSAX2ProcessingInstruction(ctx,target,data);
}
static void end(void *ctx,const xmlChar *local,const xmlChar *prefix,const xmlChar *uri){
 xmlParserCtxt *parser=ctx;dxml_parser *s=parser->_private;xmlNode *node=parser->node;
 xmlSAX2EndElementNs(ctx,local,prefix,uri);
 if(!node)return;
 int matched=(!uri||!*uri)&&(!xmlStrcmp(local,BAD_CAST "channel")||!xmlStrcmp(local,BAD_CAST "programme"));
 if(matched){
  if(s->emit(s->opaque,node)){s->capacity=1;xmlStopParser(parser);return;}
  while(node->prev){xmlNode *old=node->prev;release_node(s,old);}
 }
 if(matched||(node->parent&&node->parent->type==XML_ELEMENT_NODE&&node->parent->parent&&node->parent->parent->type==XML_DOCUMENT_NODE)){
  release_node(s,node);s->bytes=0;s->nodes=0;
 }
}
dxml_parser *dxml_new(emit_fn emit,void *opaque){
 dxml_parser *s=calloc(1,sizeof(*s));if(!s)return NULL;
 s->heap=heap_new(16*1024*1024);if(!s->heap){free(s);return NULL;}
 heap_budget *previous=current_heap;current_heap=s->heap;
 s->parser=xmlCreatePushParserCtxt(NULL,NULL,NULL,0,NULL);
 if(!s->parser){current_heap=previous;heap_release(s->heap);free(s);return NULL;}
 s->emit=emit;s->opaque=opaque;s->parser->_private=s;
 xmlCtxtUseOptions(s->parser,XML_PARSE_RECOVER|XML_PARSE_NOBLANKS|XML_PARSE_NOENT|XML_PARSE_NONET|XML_PARSE_NO_XXE|XML_PARSE_NOCDATA|XML_PARSE_COMPACT|XML_PARSE_BIG_LINES);
 s->parser->sax->startElementNs=start;s->parser->sax->endElementNs=end;
 s->parser->sax->characters=chars;s->parser->sax->cdataBlock=cdata;
 s->parser->sax->comment=comment;s->parser->sax->processingInstruction=instruction;
 s->parser->sax->warning=quiet;s->parser->sax->error=quiet;s->parser->sax->fatalError=quiet;s->parser->sax->serror=structured;
 current_heap=previous;return s;
}
int dxml_feed(dxml_parser *s,const char *bytes,int len,int final){
 if(!s->seen_root){s->dtd_input+=(size_t)len;if(s->dtd_input>1024*1024){s->capacity=1;xmlStopParser(s->parser);return 2;}}
 heap_budget *previous=current_heap;current_heap=s->heap;
 xmlParseChunk(s->parser,bytes,len,final);
 if(atomic_load(&s->heap->failed)){s->capacity=1;xmlStopParser(s->parser);}
 if(xmlDictGetUsage(s->parser->dict)>16*1024*1024){s->capacity=1;xmlStopParser(s->parser);}
 if(s->capacity){current_heap=previous;return 2;}
 if(final&&s->parser->errNo==XML_ERR_TAG_NOT_FINISHED){
  while(s->parser->node){
   xmlNode *node=s->parser->node;
   end(s->parser,node->name,node->ns?node->ns->prefix:NULL,node->ns?node->ns->href:NULL);
   if(s->capacity){current_heap=previous;return 2;}
  }
 }
 current_heap=previous;return 0;
}
void dxml_drop(dxml_parser *s){if(!s)return;heap_budget *previous=current_heap;current_heap=s->heap;xmlDoc *doc=s->parser->myDoc;xmlFreeParserCtxt(s->parser);if(doc)xmlFreeDoc(doc);current_heap=previous;heap_release(s->heap);free(s);}
int dxml_failed(dxml_parser *s){return !s||s->capacity||atomic_load(&s->heap->failed);}
const char *dxml_name(xmlNode *n){return (const char*)n->name;}
const char *dxml_uri(xmlNode *n){return n->ns?(const char*)n->ns->href:NULL;}
xmlNode *dxml_child(xmlNode *n){return xmlFirstElementChild(n);}
xmlNode *dxml_next(xmlNode *n){return xmlNextElementSibling(n);}
xmlAttr *dxml_attr(xmlNode *n){return n->properties;}
xmlAttr *dxml_attr_next(xmlAttr *a){return a->next;}
const char *dxml_attr_name(xmlAttr *a){return (const char*)a->name;}
const char *dxml_attr_uri(xmlAttr *a){return a->ns?(const char*)a->ns->href:NULL;}
char *dxml_attr_value(xmlNode *n,xmlAttr *a){return (char*)xmlNodeListGetString(n->doc,a->children,1);}
char *dxml_text(xmlNode *n){
 xmlBuffer *b=xmlBufferCreate();int seen=0;
 for(xmlNode *c=n->children;c;c=c->next){
  if(c->type!=XML_TEXT_NODE&&c->type!=XML_CDATA_SECTION_NODE)break;
  seen=1;xmlBufferCat(b,c->content);
 }
 char *out=seen?(char*)xmlStrdup(xmlBufferContent(b)):NULL;xmlBufferFree(b);return out;
}
void dxml_free(void *p){xmlFree(p);}

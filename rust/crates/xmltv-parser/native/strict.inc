/* Additive strict helper; Source/resource gates remain required before acceptance.
 * Original Dispatcharr authors retain full application/algorithm credit.
 * This intentionally shares memory.h's single TU TLS/once allocator arena.
 * Never compile a second memory.h implementation or mutate global loaders. */
static void strict_end(void *ctx,const xmlChar *local,const xmlChar *prefix,const xmlChar *uri){
 /* Keep the bounded complete root tree until strict EOF validation succeeds. */
 xmlSAX2EndElementNs(ctx,local,prefix,uri);
}
dxml_parser *dxml_strict_new(emit_fn emit,void *opaque,size_t heap_limit){
 if(heap_limit<65536||heap_limit>16*1024*1024)return NULL;
 dxml_parser *s=calloc(1,sizeof(*s));if(!s)return NULL;
 s->heap=heap_new(heap_limit);if(!s->heap){free(s);return NULL;}
 heap_budget *previous=current_heap;current_heap=s->heap;
 s->parser=xmlCreatePushParserCtxt(NULL,NULL,NULL,0,NULL);
 if(!s->parser){current_heap=previous;heap_release(s->heap);free(s);return NULL;}
 s->emit=emit;s->opaque=opaque;s->parser->_private=s;
 /* Fresh context: no RECOVER and no NOBLANKS. Local external-resource behavior
  * stays unaccepted until the owned-sentinel original-helper golden passes. */
 xmlCtxtUseOptions(s->parser,XML_PARSE_NOENT|XML_PARSE_DTDLOAD|XML_PARSE_NONET|XML_PARSE_NO_XXE|XML_PARSE_NOCDATA|XML_PARSE_COMPACT|XML_PARSE_BIG_LINES);
 s->parser->sax->startElementNs=start;s->parser->sax->endElementNs=strict_end;
 s->parser->sax->characters=chars;s->parser->sax->cdataBlock=cdata;
 s->parser->sax->comment=comment;s->parser->sax->processingInstruction=instruction;
 s->parser->sax->warning=quiet;s->parser->sax->error=quiet;s->parser->sax->fatalError=quiet;s->parser->sax->serror=structured;
 current_heap=previous;return s;
}
int dxml_strict_feed(dxml_parser *s,const char *bytes,int len,int final){
 heap_budget *previous=current_heap;current_heap=s->heap;int result=0;
 if(len<0){s->capacity=1;result=2;goto done;}
 if(!s->seen_root){s->dtd_input+=(size_t)len;if(s->dtd_input>1024*1024){s->capacity=1;xmlStopParser(s->parser);result=2;goto done;}}
 int parsed=xmlParseChunk(s->parser,bytes,len,final);
 if(atomic_load(&s->heap->failed)||xmlDictGetUsage(s->parser->dict)>16*1024*1024){s->capacity=1;xmlStopParser(s->parser);}
 if(s->capacity){result=2;goto done;}
 if(parsed||!s->parser->wellFormed){result=3;goto done;}
 if(final){
  xmlNode *root=xmlDocGetRootElement(s->parser->myDoc);
  if(!root){result=3;goto done;}
  /* Extract within active arena ownership. No borrowed node escapes callback. */
  if(s->emit(s->opaque,root)){s->capacity=1;result=2;goto done;}
  if(atomic_load(&s->heap->failed)){s->capacity=1;result=2;goto done;}
 }
done:
 current_heap=previous;return result;
}

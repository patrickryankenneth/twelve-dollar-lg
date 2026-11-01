// lg.c - native load generator following bench/load.js's user model.
// usage: ./lg HOST PORT USERS RAMP_S HOLD_S TOKENS_FILE [NSRC=8]   (env THINK_SCALE=1.0)
// TOKENS_FILE: one user per line: "<jwt> <username>"
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#ifndef IP_BIND_ADDRESS_NO_PORT
#define IP_BIND_ADDRESS_NO_PORT 24
#endif
enum { S_NONE, S_CONNECTING, S_WAIT, S_SLEEP };
enum { R_FEED, R_POST, R_LIKE, R_CREATE };
typedef struct { int fd; uint8_t state, step, req, docreate; uint32_t iter, plen; int64_t post_id, t0; char *part; } U;
typedef struct { int64_t t; uint32_t u; } H;

static U *us; static H *heap; static size_t hn;
static char **tok, **uname; static size_t ntok;
static int ep, nsrc = 8; static unsigned src_rr; static long nopen, maxopen;
static struct sockaddr_in srv; static char hostport[128];
static double scale = 1.0;
#define NB 300000
static uint32_t *hist; static uint64_t reqs, fails, neterr, maxlat;
static uint64_t rs = 88172645463325252ULL;
static uint64_t e_bind,e_conn,e_send,e_eof,e_rd,e_so,nconn,conn_us,conn_max_us,loop_max_us; static int le_bind,le_conn;

static inline double rnd(void){ rs ^= rs<<13; rs ^= rs>>7; rs ^= rs<<17; return (rs>>11)*(1.0/9007199254740992.0); }
static int64_t now_us(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return (int64_t)t.tv_sec*1000000+t.tv_nsec/1000; }
static int64_t between(double lo,double hi){ return (int64_t)((lo+rnd()*(hi-lo))*1e6*scale); }

static void hpush(int64_t t, uint32_t u){ size_t i=hn++; while(i){ size_t p=(i-1)/2; if(heap[p].t<=t)break; heap[i]=heap[p]; i=p; } heap[i].t=t; heap[i].u=u; }
static H hpop(void){ H top=heap[0]; H last=heap[--hn]; size_t i=0; for(;;){ size_t c=2*i+1; if(c>=hn)break; if(c+1<hn&&heap[c+1].t<heap[c].t)c++; if(heap[c].t>=last.t)break; heap[i]=heap[c]; i=c; } if(hn)heap[i]=last; return top; }

static void schedule(uint32_t idx, int64_t delay){ us[idx].state=S_SLEEP; hpush(now_us()+delay, idx); }
static void close_conn(uint32_t idx){ U*u=&us[idx]; if(u->fd>=0){ close(u->fd); u->fd=-1; nopen--; } free(u->part); u->part=NULL; u->plen=0; }
static void conn_failed(uint32_t idx, int midreq){ U*u=&us[idx]; close_conn(idx); if(midreq){ reqs++; fails++; neterr++; u->step=0; schedule(idx, between(3,7)); } else u->state=S_NONE; }

static void send_req(uint32_t idx, int req);
static void do_connect(uint32_t idx){
  U*u=&us[idx];
  int fd=socket(AF_INET,SOCK_STREAM|SOCK_NONBLOCK,0); if(fd<0){ conn_failed(idx,1); return; }
  int one=1; setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof one);
  if(nsrc>0){ setsockopt(fd,IPPROTO_IP,IP_BIND_ADDRESS_NO_PORT,&one,sizeof one);
    struct sockaddr_in la; memset(&la,0,sizeof la); la.sin_family=AF_INET; la.sin_addr.s_addr=htonl(0x7f000002u+(src_rr++%(unsigned)nsrc));
    if(bind(fd,(struct sockaddr*)&la,sizeof la)<0){ e_bind++; le_bind=errno; close(fd); conn_failed(idx,1); return; } }
  int64_t c0=now_us(); int r=connect(fd,(struct sockaddr*)&srv,sizeof srv); int ce=errno; int64_t cd=now_us()-c0; nconn++; conn_us+=cd; if(cd>conn_max_us)conn_max_us=cd;
  if(r<0 && ce!=EINPROGRESS){ e_conn++; le_conn=ce; close(fd); conn_failed(idx,1); return; }
  u->fd=fd; nopen++; if(nopen>maxopen)maxopen=nopen; u->state=S_CONNECTING;
  struct epoll_event e; e.events=EPOLLOUT|EPOLLIN; e.data.u32=idx;
  if(epoll_ctl(ep,EPOLL_CTL_ADD,fd,&e)<0){ conn_failed(idx,1); return; }
}
static void send_now(uint32_t idx){
  U*u=&us[idx]; char b[2600], body[400]; int n;
  size_t ti=idx%ntok;
  switch(u->req){
  case R_FEED: n=snprintf(b,sizeof b,"GET /feed HTTP/1.1\r\nHost: %s\r\nUser-Agent: lg\r\n\r\n",hostport); break;
  case R_POST: n=snprintf(b,sizeof b,"GET /posts/%lld HTTP/1.1\r\nHost: %s\r\nUser-Agent: lg\r\n\r\n",(long long)u->post_id,hostport); break;
  case R_LIKE: n=snprintf(b,sizeof b,"POST /posts/%lld/like HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n",(long long)u->post_id,hostport,tok[ti]); break;
  default: { time_t tt=time(NULL); struct tm tm; char iso[40]; gmtime_r(&tt,&tm); strftime(iso,sizeof iso,"%Y-%m-%dT%H:%M:%S.000Z",&tm);
    int bl=snprintf(body,sizeof body,"{\"body\":\"%s says hi at %s (VU %u, iter %u)\"}",uname[ti],iso,idx+1,u->iter);
    n=snprintf(b,sizeof b,"POST /posts HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",hostport,tok[ti],bl,body); } }
  u->t0=now_us(); u->state=S_WAIT;
  ssize_t s=send(u->fd,b,n,MSG_NOSIGNAL);
  if(s!=n){ e_send++; conn_failed(idx,1); }
}
static void send_req(uint32_t idx, int req){ U*u=&us[idx]; u->req=req; if(u->fd<0) do_connect(idx); else send_now(idx); }

static void record(int64_t lat, int status){
  reqs++; if(status<200||status>=400) fails++;
  uint64_t b=lat/100; if(b>=NB)b=NB-1; hist[b]++; if((uint64_t)lat>maxlat)maxlat=lat;
}
static void handle_resp(uint32_t idx, int status, const char*body, size_t bl){
  U*u=&us[idx]; record(now_us()-u->t0,status);
  switch(u->req){
  case R_FEED: {
    if(status!=200){ u->step=0; schedule(idx,between(3,7)); break; }
    int64_t ids[64]; int ni=0; static const char pat[]="{\"id\":";
    for(size_t i=0;i+6<bl&&ni<64;i++) if(!memcmp(body+i,pat,6)){ ids[ni++]=atoll(body+i+6); }
    u->post_id=ni?ids[(int)(rnd()*ni)%ni]:1; u->step=1; schedule(idx,between(3,7)); break; }
  case R_POST: u->step=2; schedule(idx,between(3,8)); break;
  case R_LIKE: if(u->docreate){ send_req(idx,R_CREATE); break; } u->step=0; u->iter++; schedule(idx,between(5,15)); break;
  default: u->step=0; u->iter++; schedule(idx,between(5,15)); }
}
static char rbuf[1<<20];
static void on_readable(uint32_t idx){
  U*u=&us[idx];
  ssize_t n=recv(u->fd,rbuf,sizeof rbuf,0);
  if(n<0){ if(errno==EAGAIN||errno==EINTR)return; e_rd++; conn_failed(idx,u->state==S_WAIT); return; }
  if(n==0){ e_eof++; conn_failed(idx,u->state==S_WAIT); return; }
  if(u->state!=S_WAIT) return;           // stray bytes while idle
  char*p=rbuf; size_t len=n;
  if(u->part){ u->part=realloc(u->part,u->plen+n); memcpy(u->part+u->plen,rbuf,n); p=u->part; len=u->plen+n; }
  char*he=memmem(p,len,"\r\n\r\n",4);
  size_t cl=0, hdr=0;
  if(he){ hdr=(he-p)+4;
    for(char*q=p;q<he;q++) if((q==p||q[-1]=='\n')&&(size_t)(he-q)>15&&!strncasecmp(q,"content-length:",15)){ cl=strtoul(q+15,NULL,10); break; } }
  if(!he||len<hdr+cl){ if(!u->part){ u->part=malloc(len); memcpy(u->part,p,len); } u->plen=len; return; }
  int status=atoi(p+9);
  handle_resp(idx,status,p+hdr,cl);
  if(u->part&&p==u->part){ free(u->part); u->part=NULL; u->plen=0; }
}
static double pct(double q){ uint64_t tot=0; for(size_t i=0;i<NB;i++)tot+=hist[i]; if(!tot)return 0; uint64_t t=(uint64_t)(q*tot),c=0; for(size_t i=0;i<NB;i++){ c+=hist[i]; if(c>=t)return (i+1)*0.1; } return NB*0.1; }

int main(int argc,char**argv){
  if(argc<7){ fprintf(stderr,"usage: %s HOST PORT USERS RAMP_S HOLD_S TOKENS_FILE [NSRC]\n",argv[0]); return 2; }
  const char*host=argv[1]; int port=atoi(argv[2]); long N=atol(argv[3]); double ramp=atof(argv[4]), hold=atof(argv[5]);
  if(argc>7) nsrc=atoi(argv[7]);
  if(getenv("THINK_SCALE")) scale=atof(getenv("THINK_SCALE"));
  snprintf(hostport,sizeof hostport,"%s:%d",host,port);
  memset(&srv,0,sizeof srv); srv.sin_family=AF_INET; srv.sin_port=htons(port); inet_pton(AF_INET,host,&srv.sin_addr);
  struct rlimit rl; getrlimit(RLIMIT_NOFILE,&rl); rl.rlim_cur=rl.rlim_max; setrlimit(RLIMIT_NOFILE,&rl);
  if(rl.rlim_cur<(rlim_t)N+100) fprintf(stderr,"WARNING: nofile limit %lu < users %ld; raise it first\n",(unsigned long)rl.rlim_cur,N);
  FILE*f=fopen(argv[6],"r"); if(!f){ perror("tokens"); return 2; }
  size_t cap=1024; tok=malloc(cap*sizeof*tok); uname=malloc(cap*sizeof*uname); char line[4096];
  while(fgets(line,sizeof line,f)){ line[strcspn(line,"\r\n")]=0; char*sp=strchr(line,' '); if(!sp)continue; *sp=0;
    if(ntok==cap){ cap*=2; tok=realloc(tok,cap*sizeof*tok); uname=realloc(uname,cap*sizeof*uname); }
    tok[ntok]=strdup(line); uname[ntok]=strdup(sp+1); ntok++; }
  fclose(f); if(!ntok){ fprintf(stderr,"no tokens\n"); return 2; }
  us=calloc(N,sizeof*us); heap=malloc(N*sizeof*heap); hist=calloc(NB,sizeof*hist);
  ep=epoll_create1(0);
  int64_t start=now_us(), end=start+(int64_t)((ramp+hold)*1e6), nextp=start+10000000;
  for(long i=0;i<N;i++){ us[i].fd=-1; us[i].step=0; hpush(start+(int64_t)(ramp*1e6*i/N)+between(0,5),(uint32_t)i); us[i].state=S_SLEEP; }
  static struct epoll_event evs[1024];
  for(;;){
    int64_t now=now_us(); if(now>=end)break;
    int64_t to=hn?(heap[0].t-now)/1000:50; if(to<0)to=0; if(to>50)to=50;
    int n=epoll_wait(ep,evs,1024,(int)to); int64_t it0=now_us();
    for(int i=0;i<n;i++){ uint32_t idx=evs[i].data.u32; U*u=&us[idx];
      if(u->fd<0) continue;
      if(u->state==S_CONNECTING){
        int err=0; socklen_t el=sizeof err; getsockopt(u->fd,SOL_SOCKET,SO_ERROR,&err,&el);
        if(err||(evs[i].events&(EPOLLERR|EPOLLHUP))){ e_so++; conn_failed(idx,1); continue; }
        if(evs[i].events&EPOLLOUT){ struct epoll_event e; e.events=EPOLLIN; e.data.u32=idx; epoll_ctl(ep,EPOLL_CTL_MOD,u->fd,&e); send_now(idx); }
      } else if(evs[i].events&(EPOLLIN|EPOLLHUP|EPOLLERR)) on_readable(idx);
    }
    now=now_us();
    while(hn&&heap[0].t<=now){ H h=hpop(); U*u=&us[h.u];
      switch(u->step){
      case 0: send_req(h.u,R_FEED); break;
      case 1: send_req(h.u,R_POST); break;
      default: { u->docreate=rnd()<0.02; if(rnd()<0.15) send_req(h.u,R_LIKE); else if(u->docreate) send_req(h.u,R_CREATE); else { u->step=0; u->iter++; schedule(h.u,between(5,15)); } } }
    }
    { int64_t d=now_us()-it0; if(d>loop_max_us)loop_max_us=d; }
    if(now>=nextp){ nextp+=10000000;
      fprintf(stderr,"lg err: bind=%lu(e%d) conn=%lu(e%d) connto=%lu eof=%lu rd=%lu send=%lu | connects=%lu avg=%.2fms max=%.1fms | loop_max=%.1fms\n",(unsigned long)e_bind,le_bind,(unsigned long)e_conn,le_conn,(unsigned long)e_so,(unsigned long)e_eof,(unsigned long)e_rd,(unsigned long)e_send,(unsigned long)nconn,nconn?conn_us/1000.0/nconn:0.0,conn_max_us/1000.0,loop_max_us/1000.0); conn_max_us=0; loop_max_us=0; fprintf(stderr,"lg t=%.0fs open=%ld reqs=%lu fail=%lu p95=%.1fms p99=%.1fms\n",(now-start)/1e6,nopen,(unsigned long)reqs,(unsigned long)fails,pct(.95),pct(.99)); }
  }
  double el=(now_us()-start)/1e6, p95=pct(.95), p99=pct(.99);
  printf("lg: users=%ld open_at_end=%ld max_open=%ld reqs=%lu (%.0f/s) failed=%lu (%.3f%%) neterr=%lu\n",N,nopen,maxopen,(unsigned long)reqs,reqs/el,(unsigned long)fails,reqs?100.0*fails/reqs:0.0,(unsigned long)neterr);
  printf("lg: p50=%.2fms p95=%.2fms p99=%.2fms max=%.1fms\n",pct(.5),p95,p99,maxlat/1000.0);
  int pass=(p95<500&&p99<1000&&(reqs==0||fails*100<reqs));
  printf("lg: %s (p95<500, p99<1000, fail<1%%)\n",pass?"PASS":"FAIL");
  return pass?0:99;
}

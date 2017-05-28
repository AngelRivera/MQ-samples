/************************************************************/
/* How to compile this sample

See the following section in the online MQ manual:
http://www.ibm.com/support/knowledgecenter/SSFKSJ_9.0.0/com.ibm.mq.dev.doc/q028490_.htm
IBM MQ > IBM MQ 9.0.x > IBM MQ > Developing applications > Developing MQI applications with IBM MQ > Building a procedural application > Building your procedural application on Linux > Preparing C programs > 
Building 64-bit applications

In particular for this sample, for Linux x86 64-bit where MQ is installed in /opt/mqm90:
  C client application, 64-bit, threaded:
    gcc -m64 -o amqsblstc-plus amqsblst-plus.c -I /opt/mqm90/inc -L /opt/mqm90/lib64 -Wl,-rpath=/opt/mqm90/lib64 -Wl,-rpath=/usr/lib64 -lmqic_r -lpthread

*/

/******************************************************************************/
/* amqsblstc-plus:  Blast with sleep and persistent                           */
/* This is an enhancement for the MQ sample:                                  */
/*     /opt/mqm/samp/amqsblst.c                                               */
/* The additional functions are:                                              */
/*   -t seconds  to sleep after each put and before commit (default; 0)       */ 
/*               it is helpful to study behavior of local unit of work and    */
/*               uncommitted messages                                         */
/*   -p          to indicate if messages are persistent                       */ 
/*               default: non persistent                                      */
/*                                                                            */
/*    <copyright                                                              */
/*    notice="lm-source-program"                                              */
/*    pids="5724-H72"                                                         */
/*    years="1994,2016"                                                       */
/*    crc="2498754217" >                                                      */
/*    Licensed Materials - Property of IBM                                    */
/*                                                                            */
/*    5724-H72                                                                */
/*                                                                            */
/*    (C) Copyright IBM Corp. 1994, 2016 All Rights Reserved.                 */
/*                                                                            */
/*    US Government Users Restricted Rights - Use, duplication or             */
/*    disclosure restricted by GSA ADP Schedule Contract with                 */
/*    IBM Corp.                                                               */
/*    </copyright>                                                            */
/*                                                                            */
/* The application is designed to either populate a queue with messages or    */
/* read them back. It will either populate a queue with 1k messages or read   */
/* them back. These messages will contain a sequence number, that starts      */
/* at 1 and which will be checked upon reading back. The sequence number of   */
/* the next message need only be higher than the preceding message. This is   */
/* for commit/rollback purposes (see below).                                  */
/*                                                                            */
/* There is a commandline argument to print diagnostics as the                */
/* application is running, either to display the messages that are being put  */
/* onto the queue or being read back.                                         */
/*                                                                            */
/* The user can specify how many messages are placed on the queue, however    */
/* if this argument is not supplied then a default of 1000 will be used.      */
/*                                                                            */
/* When this application is invoked to read the messages, the last message    */
/* placed on the queue will have an indicator to say it is the last message   */
/* expected. The message format will be:                                      */
/*       Mlast message ssssss <alphanumeric characters up to message length>  */
/* where :                                                                    */
/*   M is a letter b which is replaced with B for the last message.           */
/*   ssssss is a 6 digit number to store the sequence number.                 */
/* Note: We are only storing a 6 digit sequence number. It is possible that   */
/* this number will wrap back to 000000.                                      */
/*                                                                            */
/* By default, the status of this application will be logged at the           */
/* beginning and end of the execution cycle. For every 100 messages           */
/* written/read a status update will be written out. The status interval      */
/* can also be modified by a commandline option.                              */
/*                                                                            */
/* By default the message sizes will be 1K, however a commandline argument is */
/* provided  which randomises the message size up to the maximum allowed.     */
/*                                                                            */
/* The application can also randomly back out rather than commit a unit of    */
/* work, this can be specified on the commandline. If no UOW commandline      */
/* options is specified, then the option to randomly backout is ignored       */
/* For this method of execution, the sequence numbers may not be sequential.  */
/* This is OK as the messages only need to have sequence numbers higher than  */
/* the last message on the queue, not necessarily the next number in the      */
/* sequence.                                                                  */
/*                                                                            */
/* When the application is invoked to read messages from the queue, it will   */
/* wait indefinitely for messages to appear. However it is possible for the   */
/* user to specify a maximum wait time after which the application            */
/* will terminate gracefully.                                                 */
/*                                                                            */
/******************************************************************************/
/* Usage:                                                                     */
/*   amqsblstc-plus queue_manager queue -R -W -s message_size -c message_count -C correlid -t sleep_time -p -v
/* where:                                                                     */
/*    -W put messages to queue (Write)                                        */
/*    -R get nmessages from queue (Read)                                      */
/*    -v invokes verbose mode                                                 */
/*    -s sets message size (default is 1024, minimum is 20)                   */
/*    -c sets number of messages (default is 1000)                            */
/*    -u sets the number of units of work (default is 0)                      */
/*    -t sleep time after each put and before commit, in seconds (default 0)  */
/*    -p persistent (default is non-persistent)                               */
/* note:                                                                      */
/*    -R and -W are mutually exclusive                                        */
/******************************************************************************/

/* following line gets rid of deprecation warnings on Windows */
#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#if defined(_WIN32)
#include <windows.h>
#else
#include <time.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/resource.h>
#include <errno.h>
#include <unistd.h>
#endif
/* includes for MQI */
#include <cmqc.h>

/*********************************************************/
/* local type definitions                                */
/*********************************************************/

typedef enum open_mode {
  OUTPUT_MODE,
  INPUT_MODE,
  UNITITIALIZED_MODE
} OPEN_MODE;

#if MQAT_DEFAULT != MQAT_WINDOWS_NT
typedef unsigned int BOOLEAN;
#if !defined(FALSE)
#define FALSE 0
#endif
#if !defined(TRUE)
#define TRUE !FALSE
#endif
#endif
#define PASS 0
#define FAIL !FALSE

/*********************************************************/
/* global variables                                      */
/*********************************************************/

MQHCONN      gConnectionHandle = MQHC_UNUSABLE_HCONN;
MQHOBJ       gObjectHandle     = MQHO_UNUSABLE_HOBJ;
MQCHAR48     gQMgrName         = "";         /* use default QM */
MQCHAR48     gQueueName        = "TEST_QUEUE";
MQCHAR       gCorrelid[sizeof(MQBYTE24) + 1]
                               = MQCI_NONE;
OPEN_MODE    gMode             = UNITITIALIZED_MODE;
unsigned int gMessageSize      = 512;        /* size of message to be sent     */
unsigned int gMessageCount     = 10;         /* number of messages to be sent  */
BOOLEAN      gVerboseMode      = FALSE;
BOOLEAN      gRandom           = FALSE;      /* random message sizes           */
BOOLEAN      gSyncpoint        = FALSE;      /* get and put under syncpoint    */
unsigned int gUowSize          = 0;          /* unit of work size              */
unsigned int gGetCount         = 0;          /* total number of messages got   */
unsigned int gPutCount         = 0;          /* total number of messages put   */
unsigned int gSleepTime        = 0;          /* sleep time in seconds          */
BOOLEAN      gPersistentMode   = FALSE;      /* default is non-persistent      */
#if MQAT_DEFAULT == MQAT_WINDOWS_NT
   SYSTEMTIME   gStartTime;                  /* start time                     */
   SYSTEMTIME   gFinishTime;                 /* finish time                    */
#elif !defined(CLOCK_REALTIME)
   struct timeval gStartTime;                /* start time                     */
   struct timeval gFinishTime;               /* finish time                    */
#else
   struct timespec gStartTime;               /* start time                     */
   struct timespec gFinishTime;              /* finish time                    */
#endif

#define MAX_MESSAGE_SIZE 262144              /* maximum permitted message size */
#define END_MESSAGE 'B'                      /* end of message marker          */

/* platform dependent timing macros */

#if MQAT_DEFAULT == MQAT_WINDOWS_NT
#define startTiming() GetSystemTime(&gStartTime)
#define stopTiming()  GetSystemTime(&gFinishTime)
#elif !defined(CLOCK_REALTIME)
#define startTiming() gettimeofday(&gStartTime, NULL)
#define stopTiming()  gettimeofday(&gFinishTime, NULL)
#else
#define startTiming() clock_gettime(CLOCK_REALTIME, &gStartTime)
#define stopTiming()  clock_gettime(CLOCK_REALTIME, &gFinishTime)
#endif

/********************************************************************/
/* info()                                                           */
/********************************************************************/
/*                                                                  */
/*   function: print an information message                         */
/*                                                                  */
/*   arguments: similar to printf()                                 */
/*                                                                  */
/*   return value: none                                             */
/*                                                                  */
/********************************************************************/

void info( char * format, ... )

{
   va_list args;
   char buffer[256];

   va_start( args, format );
   vsprintf( buffer, format, args );
   printf( "Blast> %s\n", buffer );
}

/********************************************************************/
/* vinfo()                                                          */
/********************************************************************/
/*                                                                  */
/*   function: print an information message if verbose mode         */
/*                                                                  */
/*   arguments: similar to printf()                                 */
/*                                                                  */
/*   return value: none                                             */
/*                                                                  */
/********************************************************************/

void vinfo( char * format, ... )

{
   va_list args;
   int     len    = 0;
   char buffer[256];

   va_start( args, format );
   if (gVerboseMode) {
     vsprintf( buffer, format, args );
     printf( "Blast> %s\n", buffer );
   }
}

/********************************************************************/
/* connectQM()                                                     */
/********************************************************************/
/*                                                                  */
/*   function: connect to a queue manager                           */
/*                                                                  */
/*   arguments:                                                     */
/*      qm_name     : name of queue manager to connect to           */
/*      pHconn      : connection handle (returned value)            */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQCONN (0 if success)       */
/*                                                                  */
/********************************************************************/

unsigned int connectQM(char * QMName, MQHCONN * pHconn)

 {
   MQLONG   CompCode = 0;         /* completion code               */
   MQLONG   Reason   = 0;         /* reason code for MQCONN        */

   MQCONN( QMName                 /* queue manager                 */
         , pHconn                 /* connection handle             */
         , &CompCode              /* completion code               */
         , &Reason                /* reason code                   */
         );

   /* report reason */
   switch(CompCode) {
     case MQCC_WARNING:
       info("MQCONN partially completed with reason code %d", Reason);
       break;
     case MQCC_FAILED:
       info("MQCONN failed to connect to queue manager <%s> with reason code %d", QMName, Reason);
       break;
     case MQCC_OK:
       vinfo("MQCONN successfully completed");
       break;
   }
   return (unsigned int)Reason ;
 }

/********************************************************************/
/* disconnectQM()                                                   */
/********************************************************************/
/*                                                                  */
/*   function: disconnect from a queue manager                      */
/*                                                                  */
/*   arguments:                                                     */
/*      Hconn       : connection handle returned from connectQM()   */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQDISC (0 if success)       */
/*                                                                  */
/********************************************************************/

unsigned int disconnectQM(MQHCONN Hconn)

{
  MQLONG   CompCode = 0;          /* completion code               */
  MQLONG   Reason   = 0;          /* reason code for MQCONN        */

  MQDISC( &Hconn                  /* connection handle            */
        , &CompCode               /* completion code              */
        , &Reason                 /* reason code                  */
        );

  /* report reason, if any     */
  if (Reason != MQRC_NONE) {
    info("MQDISC ended with reason code %d", Reason);
  }
  return((unsigned int)Reason);
}

/********************************************************************/
/* openQueue()                                                      */
/********************************************************************/
/*                                                                  */
/*   function: open a queue for reading or writing (mode parameter) */
/*                                                                  */
/*   arguments:                                                     */
/*      Hconn       : connection handle returned from connectQM()   */
/*      Hobj        : name of queue to open                         */
/*      mode        : OUTPUT_MODE, or INPUT_MODE                    */
/*      pHobj       : returned object handle for queue              */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQOPEN (0 if success)       */
/*                                                                  */
/********************************************************************/

unsigned int openQueue( const MQHCONN  Hconn
                      , const MQCHAR48 QueueName
                      , OPEN_MODE      mode
                      , MQHOBJ *       pHobj
                      )

{
  MQOD     ObjDesc     = {MQOD_DEFAULT};     /* Object Descriptor             */
  MQLONG   OpenOptions = 0;                  /* MQOPEN options                */
  MQLONG   CompCode    = 0;                  /* completion code               */
  MQLONG   Reason      = 0;                  /* reason code for MQCONN        */

  strncpy(ObjDesc.ObjectName, QueueName, (size_t)MQ_Q_NAME_LENGTH);
  if (mode == OUTPUT_MODE) {

    OpenOptions = MQOO_OUTPUT                /* open queue for output         */
                | MQOO_FAIL_IF_QUIESCING     /* but not if MQM stopping       */
                ;                            /* = 0x2010 = 8208 decimal       */
  } else  {
    OpenOptions = MQOO_INPUT_AS_Q_DEF        /* open queue for input          */
                | MQOO_FAIL_IF_QUIESCING     /* but not if MQM stopping       */
                ;                            /* = 0x2010 = 8208 decimal       */
  }

  MQOPEN( Hconn                              /* connection handle             */
        , &ObjDesc                           /* object descriptor for queue   */
        , OpenOptions                        /* open options                  */
        , pHobj                              /* object handle                 */
        , &CompCode                          /* MQOPEN completion code        */
        , &Reason                            /* reason code                   */
        );

  /* report reason */
  switch(CompCode) {
    case MQCC_WARNING:
      info("MQOPEN partially completed with reason code %d", Reason);
      break;
    case MQCC_FAILED:
      info("MQOPEN failed with reason code %d", Reason);
      break;
    case MQCC_OK:
      break;
  }
  return (unsigned int)Reason ;
}

/********************************************************************/
/* closeQueue()                                                     */
/********************************************************************/
/*                                                                  */
/*   function: close an open queue                                  */
/*                                                                  */
/*   arguments:                                                     */
/*      Hconn  : connection handle returned from connectQM()        */
/*      Hobj   : object handle for queue returned from openQueue()  */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQCLOSE (0 if success)      */
/*                                                                  */
/********************************************************************/

unsigned int closeQueue( const MQHCONN  Hconn
                       , MQHOBJ         Hobj
                       )

{
  MQLONG   CompCode     = 0;                 /* completion code              */
  MQLONG   Reason       = 0;                 /* reason code for MQCONN       */
  MQLONG   CloseOptions = MQCO_NONE;         /* MQCLOSE options              */

  MQCLOSE( Hconn                             /* connection handle            */
         , &Hobj                             /* object handle                */
         , CloseOptions                      /* MQCLOSE options              */
         , &CompCode                         /* completion code              */
         , &Reason                           /* reason code                  */
         );

  /* report reason */
  switch(CompCode) {
    case MQCC_WARNING:
      info("MQCLOSE partially completed with reason code %d", Reason);
      break;
    case MQCC_FAILED:
      info("MQCLOSE failed with reason code %d", Reason);
      break;
    case MQCC_OK:
      break;
  }
  return (unsigned int)Reason ;
}

/********************************************************************/
/* rollBack()                                                       */
/********************************************************************/
/*                                                                  */
/*   function: Rollback the current unit of work                    */
/*                                                                  */
/*   arguments: none                                                */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQBACK (0 if success)       */
/*                                                                  */
/********************************************************************/

void rollBack(void)

{
  MQLONG   CompCode = 0;                     /* completion code               */
  MQLONG   Reason   = 0;                     /* reason code for MQCMIT        */

  MQBACK(gConnectionHandle, &CompCode, &Reason);

  /* If the rollback failed .... */
  if (CompCode != MQCC_OK) {
    info("MQBACK Rollback of changes failed - %ld", Reason);
    exit((unsigned int)Reason);
  }
}

/********************************************************************/
/* commit()                                                         */
/********************************************************************/
/*                                                                  */
/*   function: Commit the current unit of work                      */
/*                                                                  */
/*   arguments: none                                                */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQCMIT (0 if success)       */
/*                                                                  */
/********************************************************************/

unsigned int commit(void)

{
  MQLONG   CompCode = 0;                     /* completion code               */
  MQLONG   Reason   = 0;                     /* reason code for MQCMIT        */

  MQCMIT(gConnectionHandle, &CompCode, &Reason);

  /* report reason */
  switch(CompCode) {
    case MQCC_WARNING:
      info("MQCMIT partially completed with reason code %d", Reason);
      rollBack();
      break;
    case MQCC_FAILED:
      info("MQCMIT failed with reason code %d", Reason);
      rollBack();
      break;
    case MQCC_OK:
#if 0
      info("messages successfully committed <%d>", (unsigned int)Reason );
#endif
      break;
    default:
      vinfo("Blast: commit - rc = <%d>", (unsigned int)Reason );
      break;
  }

  return (unsigned int)Reason ;
}

/* define the platform dependent timing functions */

#if MQAT_DEFAULT == MQAT_WINDOWS_NT

/******************************************************************/
/* timeDiff                                                       */
/*                                                                */
/* Calculate the difference between two times - result in mS      */
/* Windows version                                                */
/******************************************************************/

void timeDiff(const SYSTEMTIME start, const SYSTEMTIME finish)

{
  FILETIME      FileTime_s;
  FILETIME      FileTime_f;
  LARGE_INTEGER start_time;
  LARGE_INTEGER finish_time;
  LARGE_INTEGER diff;

  SystemTimeToFileTime(&start, &FileTime_s);
  SystemTimeToFileTime(&finish, &FileTime_f);
  start_time.u.LowPart   = FileTime_s.dwLowDateTime;
  start_time.u.HighPart  = FileTime_s.dwHighDateTime;
  finish_time.u.LowPart  = FileTime_f.dwLowDateTime;
  finish_time.u.HighPart = FileTime_f.dwHighDateTime;
  diff.QuadPart = finish_time.QuadPart - start_time.QuadPart;
  info("elapsed time = %ld mS", (LONG) (diff.QuadPart / 1000) );
}

#elif !defined(CLOCK_REALTIME)

/******************************************************************/
/* timevalToDouble                                                */
/*                                                                */
/* Convert a timeval variable to a double (seconds)               */
/******************************************************************/

double timevalToDouble(struct timeval tv)

{
  return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

/******************************************************************/
/* timeDiff                                                       */
/*                                                                */
/* Calculate the difference between two times - result in S       */
/* UNIX gettimeofday version                                      */
/******************************************************************/

void timeDiff(const struct timeval start, const struct timeval finish)

{
  double cpuTimes, cpuTimef;
  cpuTimes = timevalToDouble(start);
  cpuTimef = timevalToDouble(finish);

  info("elapsed time = %f S", cpuTimef - cpuTimes );
}

#else

/******************************************************************/
/* timespecToDouble                                               */
/*                                                                */
/* Convert a timespec variable to a double (seconds)              */
/******************************************************************/

double timespecToDouble(struct timespec ts)

{
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

/******************************************************************/
/* timeDiff                                                       */
/*                                                                */
/* Calculate the difference between two times - result in S       */
/* UNIX clock_gettime version                                     */
/******************************************************************/

void timeDiff(const struct timespec start, const struct timespec finish)

{
  double cpuTimes, cpuTimef;
  cpuTimes = timespecToDouble(start);
  cpuTimef = timespecToDouble(finish);

  info("elapsed time = %f S", cpuTimef - cpuTimes );
}

#endif

/********************************************************************/
/* getMessage()                                                     */
/********************************************************************/
/*                                                                  */
/*   function: get a message from the opened queue                  */
/*                                                                  */
/*   arguments:                                                     */
/*      Hconn  : connection handle returned from connectQM()        */
/*      Hobj   : object handle for queue returned from openQueue()  */
/*      buffer : buffer for message                                 */
/*      buflen : length (in bytes) of message received              */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQGET (0 if success)        */
/*                                                                  */
/********************************************************************/

unsigned int getMessage( const MQHCONN Hconn
                       , const MQHOBJ  Hobj
                       , char *        buffer
                       , MQLONG        buflen
                       )

{
  MQMD     MsgDesc   = {MQMD_DEFAULT};       /* Message Descriptor            */
  MQGMO    GetMsgOpts= {MQGMO_DEFAULT};      /* get message options           */
  MQLONG   CompCode  = 0;                    /* completion code               */
  MQLONG   Reason    = 0;                    /* reason code for MQGET         */
  MQLONG   messlen   = 0;                    /* message length                */

  /******************************************************************/
  /* Use these options when connecting to Queue Managers that also  */
  /* support them, see the Application Programming Reference for    */
  /* details.                                                       */
  /* These options cause the MsgId and CorrelId to be replaced, so  */
  /* that there is no need to reset them before each MQGET          */
  /******************************************************************/
  /*GetMsgOpts.Version = MQGMO_VERSION_2;*/  /* Avoid need to reset Message   */
  /*GetMsgOpts.MatchOptions = MQMO_NONE; */  /* ID and Correlation ID after   */
                                             /* every MQGET                   */
  GetMsgOpts.Options = MQGMO_WAIT            /* wait for new messages         */
              | MQGMO_CONVERT;               /* convert if necessary          */
  GetMsgOpts.WaitInterval = 100000;          /* 100 second limit for waiting  */

  /******************************************************************/
  /* The following two statements are not required if the MQGMO     */
  /* version is set to MQGMO_VERSION_2 and and                      */
  /* GetMsgOpts.MatchOptions is set to MQGMO_NONE                   */
  /******************************************************************/
  /*                                                                */
  /*   In order to read the messages in sequence, MsgId and         */
  /*   CorrelID must have the default value.  MQGET sets them       */
  /*   to the values in for message it returns, so re-initialise    */
  /*   them before every call                                       */
  /*                                                                */
  /*   If, however, the user wishes to match the CorrelID then      */
  /*   MsgDesc.CorrelID is set to match that used to send the       */
  /*   messages                                                     */
  /******************************************************************/
  memcpy(MsgDesc.MsgId, MQMI_NONE, sizeof(MsgDesc.MsgId));
  memcpy(MsgDesc.CorrelId, MQCI_NONE, sizeof(MsgDesc.CorrelId));
  if (gCorrelid[0] != '\0') {
    memcpy(MsgDesc.CorrelId, gCorrelid, sizeof(MsgDesc.CorrelId));
  }

  /******************************************************************/
  /* In your MQ application, you can specify on every put and get   */
  /* call whether you want the call to be under syncpoint control.  */
  /* To make a put operation operate under syncpoint control, use   */
  /* the MQPMO_SYNCPOINT value in the Options field of the MQPMO    */
  /* structure when you call MQPUT. For a get operation, use the    */
  /* MQGMO_SYNCPOINT value in the Options field of the MQGMO        */
  /* structure. If you do not explicitly choose an option, the      */
  /* default action depends on the platform. The syncpoint control  */
  /* default on z/OS is yes; for all other platforms, it is no.     */
  /*                                                                */
  /* When your application receives an MQRC_BACKED_OUT reason code  */
  /* in response to an MQPUT or MQGET under syncpoint, the          */
  /* application should normally back out the current transaction   */
  /* using MBACK and then, if appropriate, retry the entire         */
  /* transaction. If the application receives MQRC_BACKED_OUT in    */
  /* response to an MQCMIT or MQDISC call, it does not need to call */
  /* MQBACK                                                         */
  /******************************************************************/

  if (gSyncpoint)
    GetMsgOpts.Options |= MQGMO_SYNCPOINT;
  else
    GetMsgOpts.Options |= MQGMO_NO_SYNCPOINT;

  /******************************************************************/
  /*                                                                */
  /*   MQGET sets Encoding and CodedCharSetId to the values in      */
  /*   the message returned, so these fields should be reset to     */
  /*   the default values before every call, as MQGMO_CONVERT is    */
  /*   specified.                                                   */
  /*                                                                */
  /******************************************************************/

  MsgDesc.Encoding       = MQENC_NATIVE;
  MsgDesc.CodedCharSetId = MQCCSI_Q_MGR;

  MQGET( Hconn                               /* connection handle                 */
       , Hobj                                /* object handle                     */
       , &MsgDesc                            /* message descriptor                */
       , &GetMsgOpts                         /* get message options               */
       , buflen                              /* buffer length                     */
       , buffer                              /* message buffer                    */
       , &messlen                            /* message length                    */
       , &CompCode                           /* completion code                   */
       , &Reason                             /* reason code                       */
       );

  /* report reason */
  switch(CompCode) {
    case MQCC_WARNING:
      info("MQGET partially completed with reason code %d", Reason);
      break;
    case MQCC_FAILED:
      info("MQGET failed with reason code %d", Reason);
      break;
    case MQCC_OK:
      vinfo("message received <%s> CorrelID <%s> length=%d"
           , buffer
           , MsgDesc.CorrelId
           , messlen
           );
      ++gGetCount;
      if (gGetCount % 100 == 0) {
        info("%d messages received", gGetCount );
      }
      break;
  }
  return (unsigned int)Reason ;
}

/********************************************************************/
/* getMessages()                                                    */
/********************************************************************/
/*                                                                  */
/*   function: get messages from queue until a final message is     */
/*              found or we timeout                                 */
/*                                                                  */
/*   arguments: none                                                */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQGET (0 if success)        */
/*                                                                  */
/********************************************************************/

unsigned int getMessages( void )

{
  unsigned int ReturnCode   = 0;                  /* return code                   */
  BOOLEAN      LastMessage  = FALSE;              /* end of messages flag          */
  char        *buffer = NULL;                     /* message buffer                */
  unsigned int counter      = 1;

  buffer = (char *)malloc(MAX_MESSAGE_SIZE+1);
  if (! buffer )
  {
     info("failed to malloc buffer of size %d", MAX_MESSAGE_SIZE );
     return 8;
  }

  startTiming();

  while(!(ReturnCode || LastMessage)) {
    ReturnCode = getMessage( gConnectionHandle
                           , gObjectHandle
                           , buffer
                           , MAX_MESSAGE_SIZE
                           );
    if (!ReturnCode) {
      /* check to see if this is the last message */
      LastMessage = (buffer[0] == END_MESSAGE);
      /* if we are getting under syncpoint we may need to commit the unit of work */
      if (((gSyncpoint) && (counter % gUowSize == 0)) || LastMessage) {
        ReturnCode = commit();
      }
      ++counter;
    } else {
      /* check for timeout */
      if (ReturnCode == MQRC_NO_MSG_AVAILABLE) {
        info("timeout out getting msgs from queue <%s>", gQueueName );
      } else {
        info("failed to get msgs from queue <%s> rc=%d", gQueueName, ReturnCode );
      }
      if (gSyncpoint) {
        rollBack();
      }
    }
  }

  stopTiming();
  timeDiff(gStartTime, gFinishTime);
  free( buffer );

  return ReturnCode;
}

/********************************************************************/
/* putMessage()                                                     */
/********************************************************************/
/*                                                                  */
/*   function: put a message to the opened queue                    */
/*                                                                  */
/*   arguments:                                                     */
/*      Hconn       : connection handle returned from connectQM()   */
/*      Hobj        : handle for queue returned from openQueue()    */
/*      MessageSize : size of message to be sent                    */
/*      buffer      : message buffer                                */
/*                                                                  */
/*   return value:                                                  */
/*      unsigned int : reason code from MQPUT (0 if success)        */
/*                                                                  */
/********************************************************************/

unsigned int putMessage( const MQHCONN      Hconn
                       , const MQHOBJ       Hobj
                       , const unsigned int MessageSize
                       ,       char *       buffer
                       )

{
  MQMD     MsgDesc      = {MQMD_DEFAULT};    /* Message Descriptor            */
  MQPMO    PutMsgOpts   = {MQPMO_DEFAULT};   /* put message options           */
  MQLONG   CompCode     = 0;                 /* completion code               */
  MQLONG   Reason       = 0;                 /* reason code for MQCONN        */
  MQLONG   messlen      = 0;                 /* message length                */

  /******************************************************************/
  /* Use these options when connecting to Queue Managers that also  */
  /* support them, see the Application Programming Reference for    */
  /* details.                                                       */
  /* These options cause the MsgId and CorrelId to be replaced, so  */
  /* that there is no need to reset them before each MQPUT          */
  /******************************************************************/
  /* PutMsgOpts.Options |= MQPMO_NEW_MSG_ID;                               */
  /* PutMsgOpts.Options |= MQPMO_NEW_CORREL_ID;                            */


  memcpy( MsgDesc.Format                     /* character string format       */
        , MQFMT_STRING
        , (size_t)MQ_FORMAT_LENGTH
        );

  /**************************************************************/
  /* The following two statements are not required if the       */
  /* MQPMO_NEW_MSG_ID and MQPMO_NEW _CORREL_ID options are used */
  /**************************************************************/
  memcpy( MsgDesc.MsgId                      /* reset MsgId to get a new one    */
        , MQMI_NONE
        , sizeof(MsgDesc.MsgId)
        );

  if (gCorrelid[0] == '\0') {
    /* reset CorrelId to get a new one */
    memcpy( MsgDesc.CorrelId
          , MQCI_NONE
          , sizeof(MsgDesc.CorrelId)
          );
  } else {
    /* use the supplied correllation ID */
    memcpy( MsgDesc.CorrelId
          , gCorrelid
          , sizeof(MsgDesc.CorrelId)
          );
  }

  /******************************************************************/
  /* In your MQ application, you can specify on every put and get   */
  /* call whether you want the call to be under syncpoint control.  */
  /* To make a put operation operate under syncpoint control, use   */
  /* the MQPMO_SYNCPOINT value in the Options field of the MQPMO    */
  /* structure when you call MQPUT. For a get operation, use the    */
  /* MQGMO_SYNCPOINT value in the Options field of the MQGMO        */
  /* structure. If you do not explicitly choose an option, the      */
  /* default action depends on the platform. The syncpoint control  */
  /* default on z/OS is yes; for all other platforms, it is no.     */
  /*                                                                */
  /* When your application receives an MQRC_BACKED_OUT reason code  */
  /* in response to an MQPUT or MQGET under syncpoint, the          */
  /* application should normally back out the current transaction   */
  /* using MBACK and then, if appropriate, retry the entire         */
  /* transaction. If the application receives MQRC_BACKED_OUT in    */
  /* response to an MQCMIT or MQDISC call, it does not need to call */
  /* MQBACK                                                         */
  /******************************************************************/

  if (gSyncpoint)
    PutMsgOpts.Options |= MQPMO_SYNCPOINT;
  else
    PutMsgOpts.Options |= MQPMO_NO_SYNCPOINT;

  if (gPersistentMode)
    MsgDesc.Persistence = MQPER_PERSISTENT;

  messlen = (MQLONG) (1 + strlen(buffer));
  if ((unsigned int)messlen < MessageSize) {
    messlen = (MQLONG) MessageSize;
  }

  MQPUT( Hconn                               /* connection handle               */
       , Hobj                                /* object handle                   */
       , &MsgDesc                            /* message descriptor              */
       , &PutMsgOpts                         /* default options (datagram)      */
       , messlen                             /* message length                  */
       , buffer                              /* message buffer                  */
       , &CompCode                           /* completion code                 */
       , &Reason                             /* reason code                     */
       );

  /* report reason */
  switch(CompCode) {
    case MQCC_WARNING:
      info("MQPUT partially completed with reason code %d", Reason);
      break;
    case MQCC_FAILED:
      info("MQPUT failed with reason code %d", Reason);
      break;
    case MQCC_OK:
      ++gPutCount;
      break;
  }
  return (unsigned int)Reason ;
}

/********************************************************************/
/* putMessages()                                                    */
/*                                                                  */
/*   function: put messages to queue                                */
/*                                                                  */
/*   arguments:                                                     */
/*      MessageSize: size of messages to send                      */
/*      Count:        number of messages to send                    */
/*                                                                  */
/*   note:                                                          */
/*      if g.random is true then random length messages are sent    */
/*                                                                  */
/********************************************************************/

unsigned int putMessages( unsigned int  MessageSize
                        , unsigned int  Count
                        )

{
  unsigned int ReturnCode = 0;
  unsigned int counter     = 1;
  char     *buffer = NULL;        /* message buffer */

  if( gRandom )
    buffer = (char *)malloc(MAX_MESSAGE_SIZE);
  else
    buffer = (char *)malloc(MessageSize);
  if (! buffer )
  {
     info("failed to malloc buffer of size %d"
         , gRandom?MAX_MESSAGE_SIZE:MessageSize );
     return 8;
  }
  memset( buffer, 0, gRandom?MAX_MESSAGE_SIZE:MessageSize);

  startTiming();

  while(!ReturnCode && (counter <= Count)) {

    sprintf(buffer, "blast message %09d", counter );
    if (counter == Count) buffer[0] = END_MESSAGE;
    if (gRandom) {
      MessageSize = (rand() % MAX_MESSAGE_SIZE + 1);
      /* message must be big enough to contain sequence number */
      if (MessageSize < 20) MessageSize = 20;
    }
    ReturnCode = putMessage( gConnectionHandle
                           , gObjectHandle
                           , MessageSize
                           , buffer
                           );
    if (!ReturnCode) {
      vinfo("successfully put msg (%d) to queue <%s>"
           , counter
           , gQueueName
           );
      if (gPutCount % 10000 == 0) {
        info("%d messages sent", gPutCount );
      }
    } else {
      info("failed to put msg (%d) to queue <%s>"
          , counter
          , gQueueName
          );
    }
/* Sleep to allow a brief period with uncommitted messages */
    if (gSleepTime > 0) {
      printf("Blast> sleeping %d seconds...\n",gSleepTime);
      sleep(gSleepTime);
    }
    if ((gSyncpoint) && (counter % gUowSize == 0)) {
      printf("Blast> committing local unit of work.\n");
      ReturnCode = commit();
    }
    ++counter;
  }
  stopTiming();
  timeDiff(gStartTime, gFinishTime);
  free( buffer );

  return(ReturnCode);
}

/********************************************************************/
/* usage()                                                          */
/********************************************************************/
/*                                                                  */
/*   function: prints the usage information                         */
/*                                                                  */
/*   arguments:     none                                            */
/*                                                                  */
/*   return value:  none                                            */
/*                                                                  */
/********************************************************************/

void usage(void)

{
  printf("Usage: amqsblstc-plus queue_manager queue -R -W -s message_size -c message_count -C correlid -t sleep_time -p -v\n");
  printf("    where\n");
  printf("      queue_manager    : queue manager name \n");
  printf("      queue            : queue to read from or write to\n");
  printf("      -R               : read messages from queue\n");
  printf("      -W               : write messages to queue\n");
  printf("      -s message_size  : size of messages to write range 1-%d or random\n", MAX_MESSAGE_SIZE);
  printf("      -c message_count : number of messages to write range 1 upwards\n");
  printf("      -C correlid      : correllation ID\n");
  printf("      -u uow_size      : unit of work size\n");
  printf("      -t seconds       : sleep time after each put (in seconds)\n");
  printf("      -p               : pertistent messages\n");
  printf("      -v               : verbose messages\n");
}

/********************************************************************/
/* processArgs()                                                    */
/********************************************************************/
/*                                                                  */
/*   function: processes command line arguments                     */
/*                                                                  */
/*   arguments:                                                     */
/*      argc:        as supplied to main()                          */
/*      argv:        as supplied to main()                          */
/*                                                                  */
/*   return value:                                                  */
/*      PASS ==> success                                            */
/*      FAIL ==> failure                                            */
/*                                                                  */
/********************************************************************/

unsigned int processArgs(int argc, char ** argv)

{
  int          Count        = 1;
  char         ValidArgs[]  = "RWuvscCtp";     /* these are the values we expect */
  char *       p            = NULL;
  char         c            = ' ';
  unsigned int ReturnVal    = PASS;
  int          Size         = 0;
  int          SleepTime    = 0;

  if (argc < 4) {
    /* error - too few arguments */
    info("command line error, too few arguments");
    ReturnVal = FAIL;
  } else {
	strncpy(gQMgrName, argv[Count++], sizeof(gQMgrName));
	strncpy(gQueueName, argv[Count++], sizeof(gQueueName));

	while ( !ReturnVal && (Count < argc)) {
	  vinfo("arg[%d] = <%s>", Count, argv[Count] );
	  if (strlen(argv[Count]) != 2) {
		info("command line error, option <%s> is not recognised", argv[Count]);
		ReturnVal = FAIL;
	  }
	  c = argv[Count][1];
	  if ((!ReturnVal) && (argv[Count][0] != '-')) {
		info("command line error, option <%s> is not recognised", argv[Count]);
		ReturnVal = FAIL;
	  }
	  if (!ReturnVal) {
		p = strchr(ValidArgs,c);
		if (p == NULL) {
		  info("command line error, option -%c is not recognised", c);
		  ReturnVal = FAIL;
		}
	  }
	  if (!ReturnVal) {
		/* valid parm*/
		switch(c) {
		case 'v':
		  /* verbose messages */
		  gVerboseMode = TRUE;
		  break;
		case 'p':
		  /* persistent messages */
		  gPersistentMode = TRUE;
		  break;
		case 'C':
		  /* correlID */
		  /* skip to next argument which should be correlID string */
		  ++Count;
		  vinfo("arg[%d] = <%s>\n\n", Count, argv[Count] );
		  strncpy(gCorrelid,argv[Count],sizeof(gCorrelid)-1);
		  break;
		case 'R':
		  /* READ mode */
		  if (gMode == UNITITIALIZED_MODE) {
		    gMode = INPUT_MODE;
		  } else {
			/* error mode already set */
			info("command line error, options -W and -R are mutually exclusive");
			ReturnVal = FAIL;
		  }
		  break;
		case 'W':
		  /* WRITE mode */
		  if (gMode == UNITITIALIZED_MODE) {
			gMode = OUTPUT_MODE;
		  } else {
			/* error mode already set */
			info("command line error, options -W and -R are mutually exclusive");
			ReturnVal = FAIL;
		  }
		  break;
		case 's':
		  /* skip to next argument which should be message size */
		  ++Count;
		  if (Count < argc) {
			if (NULL != strstr(argv[Count],"random")) {
			  /* random message sizes - set seed to be fixed value for repeatability */
			  gRandom = TRUE;
			  srand ( 1 );
			} else {
			  Size = atoi(argv[Count]);
			  if ((Size > 0) && (Size <= MAX_MESSAGE_SIZE)) {
				gMessageSize = (unsigned int)Size;
			  } else {
				info("command line error, message size <%s> is invalid", argv[Count]);
				ReturnVal = FAIL;
			  }
			}
		  } else {
			info("command line error, message size value is missing", argv[Count]);
			ReturnVal = FAIL;
		  }
		  break;
		case 'c':
		  /* skip to next argument which should be message Count */
		  ++Count;
		  if (Count < argc) {
			Size = atoi(argv[Count]);
			if ((Size > 0) && (Size <= 999999999)) {
			  gMessageCount = (unsigned int)Size;
			} else {
			  info("command line error, message count <%s> is invalid", argv[Count]);
			  ReturnVal = FAIL;
			}
		  } else {
			info("command line error, message count value is missing", argv[Count]);
			ReturnVal = FAIL;
		  }
		  break;
		case 'u':
		  /* skip to next argument which should be unit of work size */
		  ++Count;
		  Size = atoi(argv[Count]);
		  if (Count < argc) {
			if (Size > 0) {
			  gUowSize = (unsigned int)Size;
			  gSyncpoint = TRUE;
			} else {
			  info("command line error, uow size <%s> is invalid", argv[Count]);
			  ReturnVal = FAIL;
			}
		  } else {
			info("command line error, message uow size value is missing", argv[Count]);
			ReturnVal = FAIL;
		  }
		  break;
		case 't':
		  /* skip to next argument which should be sleep time */
		  ++Count;
		  SleepTime = atoi(argv[Count]);
		  if (Count < argc) {
			if (SleepTime >= 0) {
			  gSleepTime = (unsigned int)SleepTime;
			} else {
			  info("command line error, sleep time <%s> is invalid", argv[Count]);
			  ReturnVal = FAIL;
			}
		  } else {
			info("command line error, sleep time value is missing", argv[Count]);
			ReturnVal = FAIL;
		  }
		  break;
		default:
		  ReturnVal = FAIL;
		  info("command line error, option -%c is not recognised", c);
		  break;
		}
	  }
	  ++Count;
	}

	if( ReturnVal == PASS )
	{
	  if (gMode == UNITITIALIZED_MODE) {
	    /* must have either -R or -W */
	  	info("command line error, must have either -R or -W");
		ReturnVal = FAIL;
	  } else {
	    if (gUowSize > gMessageCount) {
		  /* unit of work size mustn't be larger than number of messages */
		  info("command line error, unit of work size must be < message Count");
		  ReturnVal = FAIL;                     /* error parsing */
	    }
	  }
	}
  }
  return ReturnVal;
}

/********************************************************************/
/* setDefaults()                                                    */
/********************************************************************/
/*                                                                  */
/*   function: set the deafult values for parameters that can be    */
/*           overridden from the command line                       */
/*                                                                  */
/*   arguments:                                                     */
/*      none                                                        */
/*                                                                  */
/*   return value:                                                  */
/*      none                                                        */
/*                                                                  */
/********************************************************************/

void setDefaults(void)

{
  gMode         = UNITITIALIZED_MODE;
  gMessageSize  = 1024;
  gMessageCount = 1000;
  gVerboseMode  = FALSE;
  gRandom       = FALSE;
  memcpy( gCorrelid
        , MQCI_NONE
        , sizeof(MQBYTE24)
        );
  gSyncpoint   = FALSE;
}

/*********************************************************/
/* main routine                                          */
/*********************************************************/

int main(int argc, char ** argv)

 {
   unsigned int ReturnCode  = 0;
   BOOLEAN      Connected   = FALSE;
   BOOLEAN      QueueOpened = FALSE;

   /* welcome message */
   printf( "Blast> Welcome to blast plus\n" );

   setDefaults();
   /* process any command line arguments */
   if (argc > 1) {
     ReturnCode = processArgs(argc, argv);
   } else {
     /* must have some command line args */
     ReturnCode = 1;
   }

   if (ReturnCode) {
     /* if parsing error print usage */
     usage();
     exit(ReturnCode);
   }

   /* connect to queue manager */
   ReturnCode = connectQM(gQMgrName, &gConnectionHandle);
   if (ReturnCode) {
     exit( (int)ReturnCode );
   }
   Connected = TRUE;
   vinfo("successfully Connected to queue manager <%s>", gQMgrName );

   /* open the queue for output */

   ReturnCode = openQueue( gConnectionHandle
                         , gQueueName
                         , gMode
                         , &gObjectHandle
                         );
   if (!ReturnCode) {
     info("successfully opened queue <%s>", gQueueName );
     QueueOpened = TRUE;
   } else {
     printf("Blast: failed to open queue <%s>\n", gQueueName );
   }

   if (QueueOpened  && Connected) {
     if (gMode == OUTPUT_MODE) {
       /* put messages */
       ReturnCode = putMessages( gMessageSize, gMessageCount );
       if (!ReturnCode) {
         vinfo("successfully put msgs to queue <%s>", gQueueName );
       } else {
         info("failed to put msgs to queue <%s>", gQueueName );
       }
     }
     if (gMode == INPUT_MODE) {
       /* get a message if Connected */
       ReturnCode = getMessages();
       if (!ReturnCode) {
         vinfo("successfully got msgs from queue <%s>", gQueueName );
       } else {
         info("failed to get all msgs from queue <%s>", gQueueName );
       }
     }
   }

   /* close the queue */
   if (QueueOpened  && Connected) {
     ReturnCode = closeQueue(gConnectionHandle, gObjectHandle);
     if (!ReturnCode) {
       vinfo("successfully closed queue <%s>", gQueueName );
     } else {
       info("failed to close queue <%s>", gQueueName );
     }
   }

   /* disconnect from queue manager if Connected */
   if (Connected) {
     ReturnCode = disconnectQM(gConnectionHandle);
     if (!ReturnCode) {
       vinfo("successfully disconnected from queue manager <%s>", gQMgrName );
       /* the connection handle is now invalid */
       gConnectionHandle = MQHC_UNUSABLE_HCONN;
     } else {
       info("failed to disconnected from queue manager <%s>", gQMgrName );
       /* the connection handle might be invalid but it is beyond the scope
       of this sample to handle all error conditions                        */
       gConnectionHandle = MQHC_UNUSABLE_HCONN;
     }

   }
   /* processing complete */
   info("ended" );
   info("%d messages have been put", gPutCount );
   info("%d messages have been got", gGetCount );
}



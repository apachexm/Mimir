#ifndef MIMIR_FILE_READER_H
#define MIMIR_FILE_READER_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "log.h"
#include "filesplitter.h"

namespace MIMIR_NS {

class FileReader;

typedef bool (*UserSplit)(FileReader*, void *ptr);

enum IOMethod{CLIBIO, MPIIO, CMPIIO};

// A stream contains bytes from one or multiple files. 
// The current position is etheir a byte or EOF.
// When come to end, the stream becomes empty.
class FileReader {
  public:
    static FileReader *createStream(IOMethod, int64_t, const char *,
                                 int, int, MPI_Comm, UserSplit, void *);
    static void destroyStream(FileReader *);

  public:
    FileReader(IOMethod, int64_t, const char *, int, int, 
                MPI_Comm, UserSplit, void *);
    virtual ~FileReader();


    virtual bool open_stream();
    virtual void close_stream();

    virtual char* get_byte();
    virtual char* operator*();
    virtual void next(int val=1);
    virtual void operator++();
    virtual void operator+=(int);
    virtual bool is_eof(int off=0);
    virtual bool is_empty();
    virtual bool is_tail();
    virtual char* guard();

  protected:
    virtual void read_files();
    virtual void send_tail();
    virtual bool recv_tail();

    int64_t _get_max_rsize();
    bool _file_open(const char*);
    void _file_read_at(char*, int64_t, int64_t);
    void _file_close();

    void _print_win(std::string prefix="unknown");

    struct FileWindow{
        int64_t left_file_idx;
        int64_t left_file_off;
        int64_t right_file_idx;
        int64_t right_file_off;
        int64_t left_buf_off;
        int64_t right_buf_off;
        int64_t file_count;
        int64_t total_size;
        int64_t block_size;
        int     tail_left_off;
        int     tail_right_off;
        bool    tail_done;
    };

    FileSplitter   *splitter;
    char           *inbuf;
    int64_t         inbufsize;
    char           *tailbuf;
    int             tailbufsize;
    UserSplit       splitcb;
    void           *splitptr;
    bool            iscb;
    MPI_Request     tailreq;

    int      me, nprocs;
    MPI_Comm global_comm;

    FileWindow     win;        // slide window of the files

    IOMethod iotype;

  private:

    union FilePtr{
        FILE    *c_fp;
        MPI_File mpi_fp;
    } union_fp;
};


class CoFileReader : public FileReader {
  public:
    CoFileReader(int64_t, const char*, int,
             int, MPI_Comm, UserSplit, void *);
    virtual ~CoFileReader();

    virtual bool open_stream();
    virtual void close_stream();

  protected:
    virtual void read_files();

    void _create_comm();
    void _destroy_comm();
    bool _read_group_files();

#define GROUP_SIZE  2
    MPI_Comm local_comms[GROUP_SIZE];
    int      max_comm_count[GROUP_SIZE];
    int      cur_comm_count[GROUP_SIZE];

  private:
    MPI_File    mpi_fp;
};

}

#endif

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <mpi.h>

#include "worker.h"

void Worker::sort() {
    /** Your code ... */
    // you can use variables in class Worker: 
    //n, nprocs, rank, block_len, data, last_rank
    float *recv_buffer = new float[block_len];
    void merge(float* my_data, float* recv_data, int mode, int block_len);
    int getPartner(int i_case, int rank);
      
    //进程内排序
    std::sort(data, data + block_len);

    //至多nprocs次奇偶排序即可保证有序
    for (int i = 0; i < nprocs; i++){
        int partner = getPartner(i, rank);
        if(partner >= 0 && partner < nprocs){
            MPI_Send(data, block_len, MPI_FLOAT, partner, 0, MPI_COMM_WORLD);
            MPI_Recv(recv_buffer, block_len, MPI_FLOAT, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            merge(data, recv_buffer, block_len, rank - partner);
        }
    } 
    delete []recv_buffer;
}

int getPartner(int i_case, int rank){
    if(i_case & 1){
        if(rank & 1)
            return rank + 1;
        else
            return rank - 1;
    }
    else{
        if(rank & 1)
            return rank - 1;
        else
            return rank + 1;
    }
}

void merge(float* my_data, float* recv_data, int block_len, int mode){
    int mi, ti, ri;
    float *temp = new float[block_len];

    if (mode < 0){
        mi = ri = ti = 0;
        while (ti < block_len){
            if (mi < block_len && ri < block_len){
                if (my_data[mi] >= recv_data[ri]){
                    temp[ti] = recv_data[ri];
                    ri++;
                    ti++;
                }
                else{
                    temp[ti] = my_data[mi];
                    ti++;
                    mi++;
                }
            }
            else if (mi < block_len){
                temp[ti] = my_data[mi];
                ti++;
                mi++;
            }
            else if (ri < block_len){
                temp[ti] = recv_data[ri];
                ti++;
                ri++;
            }
        }
    }
    else{
        mi = ri = ti = block_len - 1;
        while (ti >= 0){
            if (mi >= 0 && ri >= 0){
                if (my_data[mi] < recv_data[ri]){
                    temp[ti] = recv_data[ri];
                    ri--;
                    ti--;
                }
                else{
                    temp[ti] = my_data[mi];
                    ti--;
                    mi--;
                }
            }
            else if(mi >= 0){
                temp[ti] = my_data[mi];
                ti--;
                mi--;
            }
            else if(ti >= 0){
                temp[ti] = recv_data[ri];
                ri--;
                ti--;
            }
        }
    }

    for (mi = 0; mi < block_len; mi++)
        my_data[mi] = temp[mi];
    
    delete []temp;
}

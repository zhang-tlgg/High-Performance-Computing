//考虑n/nprocs不为整数
//先判断是否逆序，再传data
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

    size_t block_size = ceiling(n, nprocs);    //可能last_rank的block_len < block_size
    size_t last_size = n - block_size * (nprocs - 1);
    float recv_float;
    float *recv_buffer = new float[block_size];
    void merge(float* my_data, float* recv_data, int my_len, int recv_len, int mode);
    int getPartner(int i_case, int rank);
      
    //进程内排序
    std::sort(data, data + block_len);

    //至多nprocs次奇偶排序即可保证有序
    for (int i = 0; i < nprocs; i++){
        int partner = getPartner(i, rank);
        if(partner >= 0 && partner < nprocs){
            if(partner < rank){
                MPI_Send(&data[0], 1, MPI_FLOAT, partner, 0, MPI_COMM_WORLD);
                MPI_Recv(&recv_float, 1, MPI_FLOAT, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if(recv_float > data[0]){
                    MPI_Send(data, block_len, MPI_FLOAT, partner, 0, MPI_COMM_WORLD);
                    MPI_Recv(recv_buffer, block_size, MPI_FLOAT, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    int recv_len = (partner == nprocs - 1)? last_size: block_size;
                    merge(data, recv_buffer, block_len, recv_len, rank - partner);
                }
            }
            else{
                MPI_Send(&data[block_len - 1], 1, MPI_FLOAT, partner, 0, MPI_COMM_WORLD);
                MPI_Recv(&recv_float, 1, MPI_FLOAT, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if(recv_float < data[block_len - 1]){
                    MPI_Send(data, block_len, MPI_FLOAT, partner, 0, MPI_COMM_WORLD);
                    MPI_Recv(recv_buffer, block_size, MPI_FLOAT, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    int recv_len = (partner == nprocs - 1)? last_size: block_size;
                    merge(data, recv_buffer, block_len, recv_len, rank - partner);
                }
            }
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

void merge(float* my_data, float* recv_data, int my_len, int recv_len, int mode){
    int mi, ti, ri;
    float *temp = new float[my_len];

    if (mode < 0){
        mi = ri = ti = 0;
        while (ti < my_len){
            if (mi < my_len && ri < recv_len){
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
            else if (mi < my_len){
                temp[ti] = my_data[mi];
                ti++;
                mi++;
            }
            else if (ri < recv_len){
                temp[ti] = recv_data[ri];
                ti++;
                ri++;
            }
        }
    }
    else{
        mi = ti = my_len - 1;
        ri = recv_len - 1;
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

    for (mi = 0; mi < my_len; mi++)
        my_data[mi] = temp[mi];
    
    delete []temp;
}

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


/* structs */
struct copy_worker_argument {
	const char* source;
	int source_direct;
	const char* target;
	int target_direct;
	unsigned long long size;
	unsigned long long skip;
	unsigned long long seek;
	double* transfer_time;
	unsigned long long* tot_written;
	unsigned long long* tot_skipped; 
};

struct worker_desc {
	struct copy_worker_argument argument;
	pthread_t thread;
	double transfer_time;
	unsigned long long tot_written;
	unsigned long long tot_skipped; 
	int result;
};


/* Computes t1 - t2 and returns the result in seconds. */
double ts_diff(const struct timespec* t1, const struct timespec* t2)
{
	time_t sec_diff = t1->tv_sec - t2->tv_sec;
	long nsec_diff = t1->tv_nsec - t2->tv_nsec;

	return (double) sec_diff + (double) nsec_diff * 1e-9;
}


char* format_size(double size)
{
	char* buffer = malloc(20);
	if (!buffer)
		return buffer;

	if ((size) >= 1024. * 1024. * 1024. * 1024.)
	{
		snprintf(buffer, 20, "%.2fTiB", size / 1024. / 1024. / 1024. / 1024.);
	}
	else if ((size) >= 1024. * 1024. * 1024.)
	{
		snprintf(buffer, 20, "%.2fGiB", size / 1024. / 1024. / 1024.);
	}
	else if ((size) >= 1024. * 1024.)
	{
		snprintf(buffer, 20, "%.2fMiB", size / 1024. / 1024.);
	}
	else if ((size) >= 1024.)
	{
		snprintf(buffer, 20, "%.2fKiB", size / 1024.);
	}
	else
	{
		snprintf(buffer, 20, "%.0fB", size);
	}

	return buffer;
}


/* Returns -1 on failure, 0 if it is not a block device and 1 if it is one. */
int is_block_device(const char* path)
{
	struct stat sbuf;

	if (stat(path, &sbuf) < 0)
	{
		printf("stat of `%s' failed: %s\n", path, strerror(errno));
		return -1;
	}

	if (S_ISBLK(sbuf.st_mode))
		return 1;
	else
		return 0;
}


int parse_size(const char* str, unsigned long long* psize)
{
	size_t cnt = strlen(str);
	unsigned long long size = 1;

	if (cnt == 0)
	{
		printf("Invalid length.\n");
		return -1;
	}

	char suffix = str[cnt - 1];
	switch(suffix)
	{
		case 'K':
			size = 1024ULL;
			break;

		case 'M':
			size = 1024ULL * 1024ULL;
			break;

		case 'G':
			size = 1024ULL * 1024ULL * 1024ULL;
			break;

		case 'T':
			size = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
			break;

		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9':
			suffix = 0;
			break;

		default:
			printf("Invalid length suffix.\n");
			return -1;
	}

	/* Ensure that all characters are digits except the suffix, which has been
	 * verified above. */
	for (int i = 0; i < cnt - 1; i++)
	{
		if (str[i] < '0' || str[i] > '9')
		{
			printf("Invalid length string.\n");
			return 1;
		}
	}

	if (suffix)
	{
		char* tmp = strdup(str);
		if (!tmp)
		{
			perror("strdup");
			return -1;
		}

		tmp[cnt - 1] = '\0';
		size = size * atoll(tmp);

		free(tmp);
	}
	else
	{
		size = size * atoll(str);
	}

	*psize = size;
	return 0;
}


void* copy_worker(void* _arg)
{
	/* Unpack the argument */
	struct copy_worker_argument* arg = _arg;
	const char* source = arg->source;
	int source_direct = arg->source_direct;
	const char* target = arg->target;
	int target_direct = arg->target_direct;
	unsigned long long size = arg->size;
	unsigned long long skip = arg->skip;
	unsigned long long seek = arg->seek;
	double* transfer_time = arg->transfer_time;
    unsigned long long* tot_written = arg->tot_written;
	unsigned long long* tot_skipped = arg->tot_skipped; 


	const int buffer_size = 1024 * 1024;

	int fd_source = -1;
	int fd_target = -1;

	/* Raw buffer obtained through malloc */
	char* _buffer = NULL;

	/* Aligned buffer */
	char* buffer = NULL;

	unsigned long long total_read = 0;
	struct timespec t_start, t_stop;

	int return_code = -1;
	int eax;

	eax = O_RDONLY;
	if (source_direct)
		eax |= O_DIRECT;

	fd_source = open(source, eax);
	if (fd_source < 0)
	{
		printf("Worker: cannot open source `%s': %s\n", source, strerror(errno));
		goto ERROR;
	}

	eax = O_WRONLY;
	if (target_direct)
		eax |= O_DIRECT;

	fd_target = open(target, eax);
	if (fd_target < 0)
	{
		printf("Worker: cannot open target `%s': %s\n", target, strerror(errno));
		goto ERROR;
	}

	_buffer = malloc(buffer_size * 2);
	if (!_buffer)
	{
		printf("Worker: cannot allocate buffer: %s\n", strerror(errno));
		goto ERROR;
	}

	/* Align the buffer on the buffer's size */
	buffer = _buffer + buffer_size;
	buffer -= (intptr_t) buffer % buffer_size;

	/* Start stopwatch */
	if (clock_gettime(CLOCK_MONOTONIC, &t_start) < 0)
	{
		printf("Worker: failed to retrieve time: %s\n", strerror(errno));
		goto ERROR;
	}

	/* Skip */
	if (skip != 0)
	{
		if (lseek(fd_source, skip, SEEK_SET) < 0)
		{
			printf("Worker: failed to seek in source: %s\n", strerror(errno));
			goto ERROR;
		}
	}

	/* Seek */
	if (seek != 0)
	{
		if (lseek(fd_target, seek, SEEK_SET) < 0)
		{
			printf("Worker: failed to seek in target: %s\n", strerror(errno));
			goto ERROR;
		}
	}

	/* Read-write loop */
	for(;;)
	{
		int to_read = (size - total_read) >= buffer_size ? buffer_size : (size - total_read);

		if (to_read == 0) break;

		/* Read */
		int cnt_read = read(fd_source, buffer, to_read);

		if (cnt_read < 0)
		{
			printf("Worker: read() failed: %s\n", strerror(errno));
			goto ERROR;
		}

		if (cnt_read == 0 && errno != EINTR)
		{
			printf("Worker: no more data available.\n");
			goto ERROR;
		}

		if (cnt_read < to_read)
		{
			printf("Worker: INFO: read only %d%% of requested size.\n",
					100 * cnt_read / to_read);
		}

		total_read += cnt_read;

		unsigned long long *c=buffer;
		unsigned long long z,i=0;
		for (z=0; z<cnt_read; z+=sizeof(long long)) {   // fixme consider ull overrunning buffer boundary
//			c=buffer+z; 
//			cprintf("%16llx ",*c); if(z%32==0) printf("\n");
			if (*c==0) i++; 
			c++; 
		} 
//		printf("\n%16d read, %16lld zeros, %16ld cnt_read/sizeof(long), %16ld\n",cnt_read,i,cnt_read/sizeof(long), sizeof(long));
//		unsigned long long where=lseek(fd_target,0,SEEK_CUR);
		if ((cnt_read/sizeof(long))==i) {
//			printf("ALL ZEROES! SEEK! to_read = %i  total_read = %llu  tell %llu  diffs %llu \n",to_read,total_read,where,total_read-where);
//            printf("seeked %llu, new offset %llu \n",cnt_read,lseek(fd_target,cnt_read,SEEK_CUR));
			lseek(fd_target,cnt_read,SEEK_CUR);
			(*tot_skipped)++; 
		} else {

			/* Write */
			for (int written = 0; written < cnt_read; )
			{
				int r = write(
						fd_target,
						buffer + written,
						cnt_read - written);

				if (r < 0)
				{
					printf("Worker: write() failed: %s\n", strerror(errno));
					goto ERROR;
				}

				if (r == 0 && errno != EINTR)
				{
					printf("Worker: no more space left.\n");
					goto ERROR;
				}

				if (r < (cnt_read - written))
				{
					printf("Worker: INFO: written only %d%% of requested size.\n",
							100 * r / (cnt_read - written));
				}

				written += r;
				//printf("written %llu \n",written);
				(*tot_written)++;
			}
		}
	}

	/* Close fds s.t. syncing is contained in the counted time */
	close(fd_target);
	close(fd_source);

	/* Take time */
	if (clock_gettime(CLOCK_MONOTONIC, &t_stop) < 0)
	{
		printf("Worker: failed to retrieve time: %s\n", strerror(errno));
		goto ERROR;
	}

	*transfer_time = ts_diff(&t_stop, &t_start);

// SUCCESS:
	return_code = 0;

ERROR:
	if (fd_target >= 0)
		close(fd_target);

	if (fd_source >= 0)
		close(fd_source);

	if (_buffer)
		free(_buffer);

	return (void*) (intptr_t) return_code;
}


int main(int argc, char** argv)
{
	int source_direct = 0;
	int target_direct = 1;

	int eax;

	if (argc < 4 || argc > 7)
	{
		printf("Usage: %s <source> <target> <size> [<number of threads> [<skip> "
				"[<seek>]]]\n\n", argv[0]);

		printf("With size, skip and seek in bytes and optional suffix "
				"T,G,M,K for Tibi-, Gibi-, Mibi- or Kibibytes.\n");

		return EXIT_FAILURE;
	}


	/* Parse the size, skip and seek commandline parameters */
	unsigned long long size;
	int n_threads = 1;
	unsigned long long skip = 0;
	unsigned long long seek = 0;

	if (parse_size(argv[3], &size) < 0)
	{
		printf("Invalid size.\n");
		return EXIT_FAILURE;
	}

	if (argc >= 5)
	{
		unsigned long long tmp;

		if (parse_size(argv[4], &tmp) < 0 || tmp < 1 || tmp > 256)
		{
			printf("Invalid number of worker threads.\n");
			return EXIT_FAILURE;
		}

		n_threads = (int) tmp;
	}

	if (argc >= 6)
	{
		if (parse_size(argv[5], &skip) < 0)
		{
			printf("Invalid skip.\n");
			return EXIT_FAILURE;
		}
	}

	if (argc >= 7)
	{
		if (parse_size(argv[6], &seek) < 0)
		{
			printf("Invalid seek.\n");
			return EXIT_FAILURE;
		}
	}

	/* See if the source and target files / devices are valid. */
	const char* source = argv[1];
	const char* target = argv[2];

	if (strlen(source) == 0 || strlen(target) == 0)
	{
		printf ("Invalid source or target.\n");
		return EXIT_FAILURE;
	}

	char* fmt_size = format_size(size);
	if (!fmt_size)
	{
		perror("format_size");
		return EXIT_FAILURE;
	}

	char* fmt_size = format_size(size);
	if (!fmt_size)
	{
		perror("format_size");
		return EXIT_FAILURE;
	}

	/* Access block devices in O_DIRECT mode. */
	eax = is_block_device(source);
	if (eax < 0)
		return EXIT_FAILURE;

	source_direct = eax;

	eax = is_block_device(target);
	if (eax < 0)
		return EXIT_FAILURE;

	target_direct = eax;

	/* Try to open them */
	eax = O_RDONLY;
	if (source_direct)
		eax |= O_DIRECT;

	int fd = open(source, eax);
	if (fd < 0)
	{
		printf("Failed to open source `%s': %s\n", source, strerror(errno));
		return EXIT_FAILURE;
	}

	eax = O_WRONLY;
	if (target_direct)
		eax |= O_DIRECT;

	int fd2 = open(target, eax);
	if (fd2 < 0)
	{
		printf("Failed to open target `%s': %s\n", target, strerror(errno));
		close(fd);
		return EXIT_FAILURE;
	}

	close(fd2);
	close(fd);

	printf ("%s%s (skip %llu bytes) -> %s%s (seek %llu bytes) %llu bytes (%s)\n",
			source, source_direct ? " (O_DIRECT)" : "",
			skip,
			target, target_direct ? " (O_DIRECT)" : "",
			seek,
			size, fmt_size);

	free(fmt_size);
	fmt_size = NULL;

	/* Start worker threads. By default one. */
	int return_code = EXIT_FAILURE;

	struct worker_desc* workers = calloc(n_threads, sizeof(struct worker_desc));
	if (workers)
	{
		/* Meassure time over all infrastructural work */
		struct timespec t_start, t_stop;

		if (clock_gettime(CLOCK_MONOTONIC, &t_start) < 0)
		{
			perror("clock_gettime");
			return EXIT_FAILURE;
		}

		/* Create worker threads */
		unsigned long long size_per_thread = (size + n_threads) / n_threads;

		/* Ensure 1M alignemnt for O_DIRECT */
		size_per_thread += 1024 * 1024;
		size_per_thread -= size_per_thread % (1024 * 1024);

		unsigned long long size_assigned = 0;

		for (int i = 0; i < n_threads; i++)
		{
			workers[i].argument = (struct copy_worker_argument){
				source,
				source_direct,
				target,
				target_direct,
				size_per_thread,
				skip + size_assigned,
				seek + size_assigned,
				&workers[i].transfer_time,
				&workers[i].tot_written,
				&workers[i].tot_skipped,
			};

			if (size_assigned + workers[i].argument.size > size)
				workers[i].argument.size = size - size_assigned;

			size_assigned += workers[i].argument.size;

			int r = pthread_create(&workers[i].thread, NULL,
					copy_worker, &workers[i].argument);

			if (r != 0)
			{
				printf("pthread_create for worker thread %d failed: %s\n",
						i, strerror(r));

				exit(EXIT_FAILURE);
			}
		}

		/* Wait for workers to finish */
		for (int i = 0; i < n_threads; i++)
		{
			void* tmp;
			int r = pthread_join(workers[i].thread, &tmp);
			if (r != 0)
			{
				printf("pthread_join for worker thread %d failed: %s\n",
						i, strerror(r));

				exit(EXIT_FAILURE);
			}

			workers[i].result = (int)(intptr_t) tmp;
		}

		/* Collect results */
		int failed = 0;

		for (int i = 0; i < n_threads; i++)
		{
			if (workers[i].result < 0)
			{
				printf("Worker thread %d failed.\n", i);
				failed = 1;
				break;
			}

			double throughput = (double) workers[i].argument.size /
				workers[i].transfer_time;

			char* str = format_size(throughput);
			if (str)
			{
				printf ("Worker %d: time: %.4f, throughput: %s/s, written: %llu blocks, skipped: %llu blocks\n", 
						i, workers[i].transfer_time, str, workers[i].tot_written, workers[i].tot_skipped);

				free(str);
			}
			else
			{
				failed = 1;
				break;
			}
		}

		if (!failed)
		{
			if (clock_gettime(CLOCK_MONOTONIC, &t_stop) < 0)
			{
				perror("clock_gettime");
				return EXIT_FAILURE;
			}

			double total_time = ts_diff(&t_stop, &t_start);
			double total_throughput = (double) size / total_time;

			char* str = format_size(total_throughput);
			if (str)
			{
				printf ("total time: %.4f, total throughput: %s/s\n",
						total_time, str);

				free(str);
				return_code = EXIT_SUCCESS;
			}
			else
				perror("format_size");
		}

		free(workers);
	}
	else
	{
		perror("calloc");
		return EXIT_FAILURE;
	}

	return return_code;
}

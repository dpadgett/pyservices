/*
 * Ultra-fast standalone 4-byte IPv4 SHA-1 CIDR preimage scanner.
 * Zero external dependencies beyond libc + libpthread.
 *
 * Input (stdin): one signed 32-bit target hash per line.
 * Args (argv[1..]): CIDR specs "a.b.c.d/prefix_len"
 * Output (stdout): "<target_hash> <a.b.c.d> <cidr_arg>\n"
 */
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HT_BITS 18
#define HT_SIZE (1U << HT_BITS)
#define HT_MASK (HT_SIZE - 1U)

typedef struct {
    int32_t key;
    uint8_t used;
} ht_slot_t;

static ht_slot_t g_ht[HT_SIZE];

static void ht_insert(int32_t k) {
    uint32_t idx = ((uint32_t)k * 2654435761U) >> (32 - HT_BITS);
    while (g_ht[idx].used) {
        if (g_ht[idx].key == k) return;
        idx = (idx + 1U) & HT_MASK;
    }
    g_ht[idx].used = 1;
    g_ht[idx].key = k;
}

static inline int ht_contains(int32_t k) {
    uint32_t idx = ((uint32_t)k * 2654435761U) >> (32 - HT_BITS);
    while (g_ht[idx].used) {
        if (g_ht[idx].key == k) return 1;
        idx = (idx + 1U) & HT_MASK;
    }
    return 0;
}

#define ROL32(x, n) (((x) << (n)) | ((x) >> (32 - (n))))

static inline int32_t sha1_ipv4_le(uint32_t ip_host_order) {
    uint32_t w[80];
    w[0] = __builtin_bswap32(ip_host_order);
    w[1] = 0x80000000U;
    for (int i = 2; i < 15; i++) w[i] = 0U;
    w[15] = 32U;
    for (int i = 16; i < 80; i++) {
        uint32_t t = w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16];
        w[i] = ROL32(t, 1);
    }

    uint32_t a = 0x67452301U;
    uint32_t b = 0xEFCDAB89U;
    uint32_t c = 0x98BADCFEU;
    uint32_t d = 0x10325476U;
    uint32_t e = 0xC3D2E1F0U;

    for (int i = 0; i < 20; i++) {
        uint32_t f = (b & c) | ((~b) & d);
        uint32_t temp = ROL32(a, 5) + f + e + 0x5A827999U + w[i];
        e = d; d = c; c = ROL32(b, 30); b = a; a = temp;
    }
    for (int i = 20; i < 40; i++) {
        uint32_t f = b ^ c ^ d;
        uint32_t temp = ROL32(a, 5) + f + e + 0x6ED9EBA1U + w[i];
        e = d; d = c; c = ROL32(b, 30); b = a; a = temp;
    }
    for (int i = 40; i < 60; i++) {
        uint32_t f = (b & c) | (b & d) | (c & d);
        uint32_t temp = ROL32(a, 5) + f + e + 0x8F1BBCDCU + w[i];
        e = d; d = c; c = ROL32(b, 30); b = a; a = temp;
    }
    for (int i = 60; i < 80; i++) {
        uint32_t f = b ^ c ^ d;
        uint32_t temp = ROL32(a, 5) + f + e + 0xCA62C1D6U + w[i];
        e = d; d = c; c = ROL32(b, 30); b = a; a = temp;
    }
    return (int32_t)(0x67452301U + a);
}

typedef struct {
    uint32_t base;
    uint64_t count;
    const char *raw_arg;
} cidr_job_t;

static cidr_job_t *g_jobs = NULL;
static int g_num_jobs = 0;
static int g_next_job = 0;
static pthread_mutex_t g_job_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_out_mu = PTHREAD_MUTEX_INITIALIZER;

static void *worker_fn(void *unused) {
    (void)unused;
    for (;;) {
        int jid = -1;
        pthread_mutex_lock(&g_job_mu);
        if (g_next_job < g_num_jobs) {
            jid = g_next_job++;
        }
        pthread_mutex_unlock(&g_job_mu);
        if (jid < 0) break;

        uint32_t base = g_jobs[jid].base;
        uint64_t count = g_jobs[jid].count;
        const char *arg = g_jobs[jid].raw_arg;
        for (uint64_t i = 0; i < count; i++) {
            uint32_t ip = base + (uint32_t)i;
            int32_t h = sha1_ipv4_le(ip);
            if (ht_contains(h)) {
                pthread_mutex_lock(&g_out_mu);
                printf("%d %u.%u.%u.%u %s\n",
                       h,
                       (ip >> 24) & 255U,
                       (ip >> 16) & 255U,
                       (ip >> 8) & 255U,
                       ip & 255U,
                       arg);
                pthread_mutex_unlock(&g_out_mu);
            }
        }
    }
    return NULL;
}

int main(int argc, char **argv) {
    memset(g_ht, 0, sizeof(g_ht));
    int32_t v;
    int n_targets = 0;
    while (scanf("%d", &v) == 1) {
        ht_insert(v);
        n_targets++;
    }
    if (n_targets == 0 || argc <= 1) return 0;

    /* Split any CIDR larger than /16 into /16 chunks for balanced multithreading */
    int cap = (argc - 1) * 16;
    g_jobs = malloc(sizeof(cidr_job_t) * (size_t)cap);
    for (int a = 1; a < argc; a++) {
        unsigned b0, b1, b2, b3, plen;
        if (sscanf(argv[a], "%u.%u.%u.%u/%u", &b0, &b1, &b2, &b3, &plen) != 5) continue;
        if (plen > 32) continue;
        uint32_t base = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
        uint32_t mask = (plen == 0) ? 0U : (~0U << (32 - plen));
        base &= mask;
        uint64_t total = 1ULL << (32 - plen);
        uint64_t chunk = (total > 65536ULL) ? 65536ULL : total;
        for (uint64_t off = 0; off < total; off += chunk) {
            if (g_num_jobs == cap) {
                cap *= 2;
                g_jobs = realloc(g_jobs, sizeof(cidr_job_t) * (size_t)cap);
            }
            g_jobs[g_num_jobs].base = base + (uint32_t)off;
            g_jobs[g_num_jobs].count = (total - off < chunk) ? (total - off) : chunk;
            g_jobs[g_num_jobs].raw_arg = argv[a];
            g_num_jobs++;
        }
    }

    int n_threads = 4;
    pthread_t tids[4];
    for (int t = 0; t < n_threads; t++) {
        pthread_create(&tids[t], NULL, worker_fn, NULL);
    }
    for (int t = 0; t < n_threads; t++) {
        pthread_join(tids[t], NULL);
    }
    free(g_jobs);
    return 0;
}

/*
 * An example RDMA client side code. 
 * Author: Animesh Trivedi 
 *         atrivedi@apache.org
 */

#include "rdma_common.h"

/* These are basic RDMA resources */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_client_id = NULL;
static struct ibv_pd *pd = NULL;
static struct ibv_comp_channel *io_completion_channel = NULL;
static struct ibv_cq *client_cq = NULL;
static struct ibv_qp_init_attr qp_init_attr;
static struct ibv_qp *client_qp;
/* Memory buffer for RDMA write */
static struct ibv_mr **client_mrs = NULL;  // Array of MRs, one for each chunk
static struct rdma_buffer_attr server_metadata_attr;
static char **chunks = NULL;  // Array of 4K chunks
static int num_chunks;        // Number of 4K chunks (RDMA_BUFFER_SIZE/4K)
#define CHUNK_SIZE 4096

/* This function prepares client side connection resources for an RDMA connection */
static int client_prepare_connection(struct sockaddr_in *s_addr)
{
	int ret = -1;
	struct rdma_cm_event *cm_event = NULL;
	
	// Create event channel
	cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed, errno: %d\n", -errno);
		return -errno;
	}

	// Create RDMA id
	ret = rdma_create_id(cm_event_channel, &cm_client_id, NULL, RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating cm id failed with errno: %d\n", -errno);
		return -errno;
	}

	// Resolve address
	ret = rdma_resolve_addr(cm_client_id, NULL, (struct sockaddr*) s_addr, 2000);
	if (ret) {
		rdma_error("Failed to resolve address, errno: %d\n", -errno);
		return -errno;
	}

	ret = process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ADDR_RESOLVED, &cm_event);
	if (ret) {
		rdma_error("Failed to receive ADDR_RESOLVED event, ret = %d\n", ret);
		return ret;
	}

	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", -errno);
		return -errno;
	}

	// Resolve route
	ret = rdma_resolve_route(cm_client_id, 2000);
	if (ret) {
		rdma_error("Failed to resolve route, errno: %d\n", -errno);
		return -errno;
	}

	ret = process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ROUTE_RESOLVED, &cm_event);
	if (ret) {
		rdma_error("Failed to receive ROUTE_RESOLVED event, ret = %d\n", ret);
		return ret;
	}

	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", -errno);
		return -errno;
	}

	// Create protection domain
	pd = ibv_alloc_pd(cm_client_id->verbs);
	if (!pd) {
		rdma_error("Failed to alloc pd, errno: %d\n", -errno);
		return -errno;
	}

	// Create completion channel
	io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
	if (!io_completion_channel) {
		rdma_error("Failed to create IO completion event channel, errno: %d\n", -errno);
		return -errno;
	}

	// Create CQ
	client_cq = ibv_create_cq(cm_client_id->verbs, CQ_CAPACITY, NULL, 
							 io_completion_channel, 0);
	if (!client_cq) {
		rdma_error("Failed to create CQ, errno: %d\n", -errno);
		return -errno;
	}

	ret = ibv_req_notify_cq(client_cq, 0);
	if (ret) {
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		return -errno;
	}

	// Create QP
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.cap.max_send_wr = 1;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_wr = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.send_cq = client_cq;
	qp_init_attr.recv_cq = client_cq;

	ret = rdma_create_qp(cm_client_id, pd, &qp_init_attr);
	if (ret) {
		rdma_error("Failed to create QP, errno: %d\n", -errno);
		return -errno;
	}
	client_qp = cm_client_id->qp;

	// Calculate number of chunks
	num_chunks = RDMA_BUFFER_SIZE / CHUNK_SIZE;
	
	// Allocate array of chunk pointers
	chunks = calloc(num_chunks, sizeof(char*));
	if (!chunks) {
		rdma_error("Failed to allocate chunks array\n");
		return -ENOMEM;
	}
	
	// Allocate array of MRs
	client_mrs = calloc(num_chunks, sizeof(struct ibv_mr *));
	if (!client_mrs) {
		rdma_error("Failed to allocate MR array\n");
		return -ENOMEM;
	}

	// Allocate and initialize each chunk
	for (int i = 0; i < num_chunks; i++) {
		chunks[i] = malloc(CHUNK_SIZE);
		if (!chunks[i]) {
			rdma_error("Failed to allocate chunk %d\n", i);
			return -ENOMEM;
		}
		// Fill each chunk with a unique string
		snprintf(chunks[i], CHUNK_SIZE, "This is chunk %d", i);
	}
	
	// Register each chunk separately
	for (int i = 0; i < num_chunks; i++) {
		client_mrs[i] = rdma_buffer_register(pd, chunks[i], CHUNK_SIZE,
											IBV_ACCESS_LOCAL_WRITE);
		if (!client_mrs[i]) {
			rdma_error("Failed to register chunk %d\n", i);
			return -ENOMEM;
		}
	}

	return 0;
}

static int connect_to_server()
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	struct rdma_buffer_attr *remote_metadata;
	int ret = -1;

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.initiator_depth = 1;
	conn_param.responder_resources = 1;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;

	ret = rdma_connect(cm_client_id, &conn_param);
	if (ret) {
		rdma_error("Failed to connect to remote host, errno: %d\n", -errno);
		return -errno;
	}

	ret = process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ESTABLISHED, &cm_event);
	if (ret) {
		rdma_error("Failed to get cm event, errno: %d\n", -errno);
		return -errno;
	}

	// Get server's buffer information from private data
	remote_metadata = (struct rdma_buffer_attr *)cm_event->param.conn.private_data;
	server_metadata_attr = *remote_metadata;

	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", -errno);
		return -errno;
	}

	printf("Connected to server. Remote buffer addr: %p, rkey: %u\n",
		   (void*)server_metadata_attr.address, server_metadata_attr.rkey);
	return 0;
}

static int client_write_chunks()
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	struct ibv_wc wc;
	int ret;

	// Write each chunk
	for (int i = 0; i < num_chunks; i++) {
		// Prepare scatter/gather entry
		memset(&sge, 0, sizeof(sge));
		sge.addr = (uint64_t)chunks[i];
		sge.length = CHUNK_SIZE;
		sge.lkey = client_mrs[i]->lkey;

		// Prepare send work request
		memset(&wr, 0, sizeof(wr));
		wr.wr_id = 0;
		wr.opcode = IBV_WR_RDMA_WRITE;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.send_flags = IBV_SEND_SIGNALED;
		wr.wr.rdma.remote_addr = server_metadata_attr.address + (i * CHUNK_SIZE);
		wr.wr.rdma.rkey = server_metadata_attr.rkey;

		// Post send
		ret = ibv_post_send(client_qp, &wr, &bad_wr);
		if (ret) {
			rdma_error("Failed to post send request, errno: %d\n", -errno);
			return ret;
		}

		// Wait for completion
		ret = process_work_completion_events(io_completion_channel, &wc, 1);
		if (ret != 1) {
			rdma_error("Failed to get work completion, ret = %d\n", ret);
			return ret;
		}

		printf("Chunk %d written successfully\n", i);
	}

	printf("All chunks written successfully\n");
	return 0;
}

/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources.
 */
static int client_disconnect_and_clean()
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/* active disconnect from the client side */
	ret = rdma_disconnect(cm_client_id);
	if (ret) {
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		//continuing anyways
	}
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_DISCONNECTED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n",
				ret);
		//continuing anyways 
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", 
			       -errno);
		//continuing anyways
	}
	/* Destroy QP */
	rdma_destroy_qp(cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(client_cq);
	if (ret) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(io_completion_channel);
	if (ret) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy memory buffers */
	if (client_mrs) {
		for (int i = 0; i < num_chunks; i++) {
			if (client_mrs[i]) {
				rdma_buffer_deregister(client_mrs[i]);
			}
		}
		free(client_mrs);
	}
	/* Free all chunks */
	if (chunks) {
		for (int i = 0; i < num_chunks; i++) {
			free(chunks[i]);
		}
		free(chunks);
	}
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Client resource clean up is complete \n");
	return 0;
}

void usage() {
	printf("Usage:\n");
	printf("rdma_client: [-a <server_addr>] [-p <server_port>] -s string (required)\n");
	printf("(default IP is 127.0.0.1 and port is %d)\n", DEFAULT_RDMA_PORT);
	exit(1);
}

int main(int argc, char **argv) {
	struct sockaddr_in server_sockaddr;
	int ret, option;

	memset(&server_sockaddr, 0, sizeof(server_sockaddr));
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);

	printf("Attempting to connect to server at %s:%d\n", 
		   inet_ntoa(server_sockaddr.sin_addr),
		   ntohs(server_sockaddr.sin_port));

	while ((option = getopt(argc, argv, "a:p:")) != -1) {
		switch (option) {
			case 'a':
				ret = get_addr(optarg, (struct sockaddr*) &server_sockaddr);
				if (ret) {
					rdma_error("Invalid IP\n");
					return ret;
				}
				break;
			case 'p':
				server_sockaddr.sin_port = htons(strtol(optarg, NULL, 0));
				break;
			default:
				usage();
				break;
		}
	}

	ret = client_prepare_connection(&server_sockaddr);
	if (ret) {
		rdma_error("Failed to setup client connection, ret = %d\n", ret);
		return ret;
	}

	ret = connect_to_server();
	if (ret) {
		rdma_error("Failed to connect to server, ret = %d\n", ret);
		return ret;
	}

	ret = client_write_chunks();
	if (ret) {
		rdma_error("Failed to write chunks to remote memory, ret = %d\n", ret);
		return ret;
	}

	ret = client_disconnect_and_clean();
	if (ret) {
		rdma_error("Failed to clean up resources, ret = %d\n", ret);
		return ret;
	}

	return 0;
}


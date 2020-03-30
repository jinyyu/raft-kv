#include <stdio.h>
#include <glib.h>
#include <stdint.h>
#include <raft-kv/common/log.h>
#include <raft-kv/server/RaftNode.h>

static uint64_t g_id = 0;
static const char* g_cluster = NULL;
static uint16_t g_port = 0;

int main(int argc, char* argv[]) {
  GOptionEntry entries[] = {
      {"id", 'i', 0, G_OPTION_ARG_INT64, &g_id, "node id", NULL},
      {"cluster", 'c', 0, G_OPTION_ARG_STRING, &g_cluster, "comma separated cluster peers", NULL},
      {"port", 'p', 0, G_OPTION_ARG_INT, &g_port, "key-value server port", NULL},
      {NULL}
  };

  GError* error = NULL;
  GOptionContext* context = g_option_context_new("usage");
  g_option_context_add_main_entries(context, entries, NULL);
  if (!g_option_context_parse(context, &argc, &argv, &error)) {
    fprintf(stderr, "option parsing failed: %s\n", error->message);
    exit(EXIT_FAILURE);
  }
  fprintf(stderr, "id:%lu, port:%d, cluster:%s\n", g_id, g_port, g_cluster);

  if (g_id == 0 || g_port == 0) {
    char* help = g_option_context_get_help(context, true, NULL);
    fprintf(stderr, help);
    free(help);
    exit(EXIT_FAILURE);
  }

  kv::RaftNode::main(g_id, g_cluster, g_port);
  g_option_context_free(context);
}

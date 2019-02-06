#include <stdio.h>
#include <glib.h>
#include <stdint.h>
#include "kvd_log.h"

int main(int argc, char* argv[])
{
    uint64_t id = 0;
    const char* cluster = NULL;
    uint16_t port = 0;
    const char* log_config = NULL;

    GOptionEntry entries[] =
        {
            {"id", 'i', 0, G_OPTION_ARG_INT64, &id, "cluster", NULL},
            {"cluster", 'c', 0, G_OPTION_ARG_STRING, &cluster, "comma separated cluster peers", NULL},
            {"port", 'p', 0, G_OPTION_ARG_INT, &port, "key-value server port", NULL},
            {"log", 'l', 0, G_OPTION_ARG_STRING, &log_config, "log configure path", NULL},
            {NULL}
        };

    GError* error = NULL;
    GOptionContext* context = g_option_context_new("usage");
    g_option_context_add_main_entries(context, entries, NULL);
    if (!g_option_context_parse(context, &argc, &argv, &error)) {
        fprintf(stderr, "option parsing failed: %s\n", error->message);
        exit(EXIT_FAILURE);
    }

    if (id == 0 || !cluster || port == 0 ||  !log_config) {
        char* help = g_option_context_get_help(context, true, NULL);
        fprintf(stderr, help);
        free(help);
        exit(EXIT_FAILURE);
    }

    g_option_context_free(context);

    LOG_DEBUG("hihi");


}

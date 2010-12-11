import optparse
import default

def set_config(option, opt_str, value, parser, func):
    try:
        func(value)
    except Exception, exc:
        raise optparse.OptionError("can't open config: %s" % exc)

def update_optparse(parser, allow_short=True):
    """Update optparse instance to support uniformity in all
    user scripts.

    The routine add the following options:
        -r, --radist-conf --- radist configuration file location;
        -x, --ix-conf --- ixServers file location.

    If you call update_optparse with argument False allow_short,
    the '-r' and '-x' option wouldn't be registered.
    """
    r_opts = dict(action="callback", type='str',
        callback=set_config, nargs=1, callback_args=(default.set_r, ),
        help="use file as radist config",)
    ix_opts = dict(action="callback", type='str',
        callback=set_config, nargs=1, callback_args=(default.set_ix, ),
        help="use file as ixServers config",)

    parser.add_option('-r', '--radist-conf', **r_opts)
    parser.add_option('-x', '--ix-conf', **ix_opts)

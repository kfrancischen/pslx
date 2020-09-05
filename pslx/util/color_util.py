class ColorsUtil(object):
    RESET = '\033[0m'
    BOLD = '\033[01m'
    DISABLE = '\033[02m'
    UNDERLINE = '\033[04m'
    REVERSE = '\033[07m'
    STRIKE_THROUGH = '\033[09m'
    INVISIBLE = '\033[08m'

    class Foreground(object):
        BLACK = '\033[30m'
        RED = '\033[31m'
        GREEN = '\033[32m'
        ORANGE = '\033[33m'
        BLUE = '\033[34m'
        PURPLE = '\033[35m'
        CYAN = '\033[36m'
        LIGHT_GREY = '\033[37m'
        DARK_GREY = '\033[90m'
        LIGHT_READ = '\033[91m'
        LIGHT_GREEN = '\033[92m'
        YELLOW = '\033[93m'
        LIGHT_BLUE = '\033[94m'
        PINK = '\033[95m'
        LIGHT_CYAN = '\033[96m'

    class Background(object):
        BLACK = '\033[40m'
        RED = '\033[41m'
        GREEN = '\033[42m'
        ORANGE = '\033[43m'
        BLUE = '\033[44m'
        PURPLE = '\033[45m'
        CYAN = '\033[46m'
        LIGHT_GREY = '\033[47m'

    @classmethod
    def make_foreground_green(cls, text):
        return ColorsUtil.Foreground.GREEN + text + ColorsUtil.RESET

    @classmethod
    def make_foreground_yellow(cls, text):
        return ColorsUtil.Foreground.YELLOW + text + ColorsUtil.RESET

    @classmethod
    def make_foreground_red(cls, text):
        return ColorsUtil.Foreground.RED + text + ColorsUtil.RESET

    @classmethod
    def make_foreground_light_grey(cls, text):
        return ColorsUtil.Foreground.LIGHT_GREY + text + ColorsUtil.RESET

    @classmethod
    def make_text_bold(cls, text):
        return ColorsUtil.BOLD + text + ColorsUtil.RESET

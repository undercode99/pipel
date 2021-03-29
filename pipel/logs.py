

import sys

class colors: 
    reset='\033[0m'
    bold='\033[01m'
    disable='\033[02m'
    underline='\033[04m'
    reverse='\033[07m'
    strikethrough='\033[09m'
    invisible='\033[08m'
    class fg: 
        black='\033[30m'
        red='\033[31m'
        green='\033[32m'
        orange='\033[33m'
        blue='\033[34m'
        purple='\033[35m'
        cyan='\033[36m'
        lightgrey='\033[37m'
        darkgrey='\033[90m'
        lightred='\033[91m'
        lightgreen='\033[92m'
        yellow='\033[93m'
        lightblue='\033[94m'
        pink='\033[95m'
        lightcyan='\033[96m'
    class bg: 
        black='\033[40m'
        red='\033[41m'
        green='\033[42m'
        orange='\033[43m'
        blue='\033[44m'
        purple='\033[45m'
        cyan='\033[46m'
        lightgrey='\033[47m'


class Logs:

    def error(self,msg):
        print(f"{colors.fg.lightred}{msg}{colors.reset}",file=sys.stderr)
    
    def success(self, msg):
        print(f"{colors.fg.lightgreen}{msg}{colors.reset}",file=sys.stdout)

    def warning(self, msg):
        print(f"{colors.fg.yellow}{msg}{colors.reset}",file=sys.stdout)
    
    def info(self, msg):
        print(f"{colors.fg.lightblue}{msg}{colors.reset}",file=sys.stdout)
    
    def default(self, msg):
        print(f"{msg}",file=sys.stdout)

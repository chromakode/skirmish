import sys
import os.path
import socket
import subprocess
from urlparse import urlparse, ParseResult
from optparse import OptionParser

BUF_SIZE = 1024
DEFAULT_IMCS_PORT = 3589
VERBOSE = False
VERSION = "0.1"

class Color:
    def __init__(self, short, name, val):
        self.short = short
        self.name = name
        self.val = val
        
    def __str__(self):    return self.name
    def __int__(self):    return self.val
    def __index__(self):  return self.val

WHITE = Color("W", "White", 0)
BLACK = Color("B", "Black", 1)
WHITE.invert = BLACK
BLACK.invert = WHITE

def read_color(val):
    val = val.lower()
    for color in (WHITE, BLACK):
        if val == color.short.lower() or val == color.name.lower() or val == color.val:
            return color

class ProtocolError(Exception):
    def __init__(self, resp, explain=None):
        self.resp = resp
        self.explain = explain
    
    def __str__(self):
        if self.explain:
            return "%s; %s" % (repr(self.resp), self.explain)
        else:
            return repr(self.resp)

class ExpectedCodeError(ProtocolError):
    def __init__(self, code, expected, resp, explain=None):
        explain = explain or "expected code: %s" % expected
        ProtocolError.__init__(self, resp, explain)
        self.code = code
        self.expected = expected

class InvalidServerError(Exception): pass
class InvalidURLError(Exception): pass
class GameNotFoundError(Exception): pass
class AuthenticationError(Exception): pass
class MoveError(Exception): pass

def parse_resp(resp):
    code, sep, msg = resp.partition(" ")
    if sep:
        try:
            code = int(code)
        except ValueError:
            pass
            
        return code, msg, resp
    else:
        return None, msg, resp

def make_resp(code, msg):
    return "%s %s\n" % (code, msg)

def log(section, text, verbose=False, line_prefix=""):
    if not verbose or VERBOSE:
        section_prefix = ("[%s] " % section) + line_prefix
        text = text.strip().replace("\n", "\n"+section_prefix)
        print section_prefix + text
  
class Player(object):
    def __init__(self, name):
        self.name = name
        
    def _log(self, text, verbose=False, line_prefix=""):
        log(self.name, text, verbose, line_prefix)
        
    def get_move(self, msg): raise NotImplementedError
    def send_move(self, move): raise NotImplementedError
    def get_result(self, move): raise NotImplementedError

def ProcessPlayer(command):
    process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    command_name = os.path.basename(command.partition(" ")[0])
    return IOPlayer("Process:%s" % command_name, process.stdout, process.stdin)

class IOPlayer(Player):
    def __init__(self, name="IOPlayer", in_stream=None, out_stream=None):
        Player.__init__(self, name)
        self.in_stream = in_stream or sys.stdin
        self.out_stream = out_stream or sys.stdout

    def skip_until_code(self, *codes):
        while True:
            line = self.in_stream.readline()
            if not line:
                break
            
            code, msg, resp = parse_resp(line)
            self._log(resp, True, "-> ")
            if code in codes:
                return code, msg, resp
            
    def write(self, data):
        self._log(data, True, "<- ")
        self.out_stream.write(data)

    def get_move(self, msg):
        self.write(make_resp("?", msg))
        code, msg, resp = self.skip_until_code("!", "=")
        if   code == "!": return msg.strip()
        elif code == "=": return resp
        
    def send_move(self, move):
        code, msg, resp = self.skip_until_code("?")
        self.write(make_resp("!", move))
        
    def get_result(self):
        code, msg, resp = self.skip_until_code("=")
        return resp

class ServerPlayer(Player):
    def __init__(self, name, server):
        Player.__init__(self, name)
        self.server = server

    def get_move(self, msg):
        try:
            return self.server.expect_resp("!")
        except ExpectedCodeError, e:
            if e.code == "=":
                return e.resp
            else:
                raise e
    
    def send_move(self, move):
        self.server.expect_resp("?")
        return self.server.send(move + "\n")
        
    def get_result(self):
        return self.server.expect_resp("=")

class IMCSServer(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
        self.username = None

    @property
    def name(self):
        return "IMCS" + (":" + self.username if self.username else "")

    def _log(self, text, verbose=False, line_prefix=""):
        log(self.name, text, verbose, line_prefix)
   
    def get_data(self):
        data = self.socket.recv(BUF_SIZE)
        self._log(data, True, "-> ")
        return data
    
    def get_resp(self):
        resp = self.get_data()
        
        # Get last line
        lines = filter(None, resp.split("\n"))
        return lines[-1] if lines else None

    def expect_resp(self, expected, explain=None, resp=None):
        resp = resp or self.get_resp()
        code, msg, resp = parse_resp(resp)
        if code == expected:
            return msg
        else:
            raise ExpectedCodeError(code, expected, resp, explain)
        
    def send(self, data):
        self._log(data, True, "<- ")
        return self.socket.send(data)
            
    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.expect_resp(100)

    def disconnect(self):
        self.send("quit\n")
        self.socket.close()
        
    def login(self, username, password):
        self.send("me %s %s\n" % (username, password))
        
        code, msg, resp = parse_resp(self.get_resp())
        if code == 201:
            self._log("Logged in as \"%s\"." % username)
            self.username = username
        elif code == 401:
            raise AuthenticationError(msg)
        else:
            raise ProtocolError(resp)
            
    def register(self, username, password):
        self.send("register %s %s\n" % (username, password))
        
        code, msg, resp = parse_resp(self.get_resp())
        if code == 202:
            self._log("Registered new user \"%s\"." % username)
        elif code == 402:
            raise AuthenticationError(msg)
        else:
            raise ProtocolError(resp)

    def list_games(self):
        self.send("list\n")
        self.expect_resp(211)
        lines = filter(None, self.get_data().split("\n"))
        
        games = list()
        for line in lines:
            if line != ".":
                try:
                    indent, gameid, name, color, rating = line.split(" ")
                    gameid = int(gameid)
                    color = read_color(color)
                    rating = int(gameid)
                    games.append({"id":gameid, "name":name, "color":color, "rating":rating})
                    
                except ValueError:
                    raise ProtocolError(line, explain="unable to parse game listing.")
            else:
                break
         
        return games
        
    def _make_player(self):
        return ServerPlayer(self.name, self)
    
    def offer(self, color):
        self.send("offer %s\n" % color)
        try:
            msg = self.expect_resp(101)
            gameid = msg.split(" ")[1]
        except:
            gameid = "???"
        self._log("Offered new game as color %s (id: %s)." % (color, gameid))
        
        self.expect_resp(102)
        self._log("Offer accepted!")
        return self._make_player()

    def accept(self, gameid):
        self.send("accept %s\n" % gameid)
        
        code, msg, resp = parse_resp(self.get_resp())
        if code == 103:
            self._log("Accepted offer with ID %s" % gameid)
        elif code == 408:
            raise GameNotFoundError(msg)
        else:
            raise ProtocolError(resp)
            
        return self._make_player()

def parse_imcs_url(urlstr):
    if urlstr.startswith("imcs:"):
        urlstr = urlstr[5:]
    
    return urlparse(urlstr, "http")

def connect_imcs_url(url):
    if not isinstance(url, ParseResult):        
        url = parse_imcs_url(url)
    
    port = url.port or DEFAULT_IMCS_PORT
    if url.hostname:
        server = IMCSServer(url.hostname, int(port))
    else:
        raise InvalidURLError("Missing hostname.")

    server.connect()
    if url.username:
        server.login(url.username, url.password)
    
    return server
    
def play_imcs_url(color, urlstr):
    url = parse_imcs_url(urlstr)
    server = connect_imcs_url(url)
    
    if url.path=="/offer":
        return server.offer(color.invert.short)
    elif url.path=="/accept" or url.path=="/":
        query = dict(pair.split("=") if not pair.startswith("rating") else ("rating",pair)
                     for pair in filter(None, url.query.split("&")))
        if "id" in query:
            return server.accept(query["id"])
        else:
            # Find a game that matches the constraints in the query
            constraints = list()
            constraints.append(lambda game: game["color"]==color)
            if "name" in query:
                constraints.append(lambda game: game["name"]==query["name"])
            elif "rating" in query:
                # FIXME: eval is possibly unsafe for untrusted queries
                constraints.append(lambda game: eval(query["rating"],
                                   {"rating":game["rating"], "__builtins__":None}))
                
            for game in reversed(server.list_games()):
                if all(constraint(game) for constraint in constraints):
                    return server.accept(game["id"])
            else:
                raise GameNotFoundError
                
    else:
        raise InvalidURLError("invalid IMCS path.")

def parse_game_result(resp):
    parts = resp.lower().split(" ")
    if parts[0] == "=":
        parts.pop(0)
    else:
        raise ProtocolError(resp)
        
    if resp.find("draw") != -1:
        return None
    else:
        return read_color(parts[0])

def game_loop(white_player, black_player, strict=False):
    players = (white_player, black_player)
    current = WHITE
    next_move = None
    
    while True:
        log("Game", "Getting move for %s" % current.name.lower())
        move = players[current].get_move("%s to move." % current)
        
        if move.startswith("="):
            cur_result = parse_game_result(move)
            
            if strict:
                opp_result = parse_game_result(players[current.invert].get_result())
                if cur_result != opp_result:
                    print "Warning! Player game end states do not agree"
                    print "\t%s: %s" % (current, repr(move))
                    print "\t%s: %s" % (current.invert, repr(oppmove))                
            
            if cur_result is not None:
                print "%s wins." % cur_result
            else:
                print "The game is a draw."
            return
        
        log("Game", "Sending move \"%s\" to %s" % (move, current.invert.name.lower()))
        players[current.invert].send_move(move)        
        current = current.invert

player_help = \
"""
Players white and black can be one of the following options:
    
  "-"                       (communicate with standard in/out)
    
  "run COMMAND"             (use standard in/out of the specified shell command)
    
  "imcs://user:pass@host:port/PATH", where path is one of:
      /offer                (offer a game of the player's color)
      /accept?arg=val[&..]  (accept game with the following allowed parameters)
        id=N                  (game has id number N)
        name=STR              (player has name STR)
        rating=N              (player has rating N)
"""
    
def main():  
    parser = OptionParser(usage="usage: %prog [options] white black", version=("%prog " + VERSION), add_help_option=False)
    parser.add_option("-h", "--help",
                      action="store_true", dest="help",
                      help="show this help message and exit")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose",
                      help="print verbose output")
    parser.add_option("-s", "--strict",
                      action="store_true", dest="strict",
                      help="perform sanity checks")

    (options, args) = parser.parse_args()
    
    if options.help:
        parser.print_help()
        print player_help
        return
    
    if len(args) != 2:
        parser.error("incorrect number of arguments.")
    
    if options.verbose:
        global VERBOSE
        VERBOSE = True
    
    def parse_player(color, text):        
        if text.startswith("imcs"):
            try:
                return play_imcs_url(color, text)
            except InvalidURLError, e:
                parser.error("invalid imcs url: %s" % e)
            except GameNotFoundError:
                print "Unable to find a suitable offer."
                sys.exit()

        elif text.startswith("run "):
            return ProcessPlayer(text.partition(" ")[2])
        
        elif text == "-":
            return IOPlayer()
            
        else:
            parser.error("invalid player specified.")

    game_loop(strict=options.strict, *map(parse_player, (WHITE, BLACK), args))

if __name__ == "__main__":
    main()

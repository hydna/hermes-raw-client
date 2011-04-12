// 
//        Copyright 2010 Hydna AB. All rights reserved.
//
//  Redistribution and use in source and binary forms, with or without 
//  modification, are permitted provided that the following conditions 
//  are met:
//
//    1. Redistributions of source code must retain the above copyright 
//       notice, this list of conditions and the following disclaimer.
//
//    2. Redistributions in binary form must reproduce the above copyright 
//       notice, this list of conditions and the following disclaimer in the 
//       documentation and/or other materials provided with the distribution.
//
//  THIS SOFTWARE IS PROVIDED BY HYDNA AB ``AS IS'' AND ANY EXPRESS OR IMPLIED
//  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
//  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
//  EVENT SHALL HYDNA AB OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
//  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT 
//  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
//  USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
//  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
//  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//  The views and conclusions contained in the software and documentation are
//  those of the authors and should not be interpreted as representing 
//  official policies, either expressed or implied, of Hydna AB.
//

var Buffer                = require("buffer").Buffer
  , Stream                = require("net").Stream
  , inherits              = require("util").inherits

// Handshake related constants
var HANDSHAKE_HEADER      = "\x44\x4E\x41\x31"
    HANDSHAKE_SIZE        = HANDSHAKE_HEADER.length + 1
    HANDSHAKE_CODE_OFF    = 0x04;

// Stream modes
var READ                  = 0x01
  , WRITE                 = 0x02
  , READWRITE             = 0x03
  , EMIT                  = 0x04;

// Opcodes
var OPEN                  = 0x01
  , DATA                  = 0x02
  , SIGNAL                = 0x03;

var MODE_RE = /^(r|read){0,1}(w|write){0,1}(?:\+){0,1}(e|emit){0,1}$/i;

exports.createConnection = function(port, host, options, callback) {
  var connection;

  if (typeof options == "function") {
    callback = hostname;
    connection = new Connection({});
    connection.connect(port, host);
  } else {
    connection = new Connection(options || {});
    connection.connect(port, host);
  }

  if (callback) {
    connection.once("handshake", callback);
  }

  return connection;
};


function Connection(options) {
  Stream.call(this, options);

  this._openCallbacks = {};

  this._readbuffer = null;
  this._readpos = 0;
  this._readend = 0;

  this._throwErrors = "throwErrors" in options ? options.throwErrors : true;
  this._hostname = "hostname" in options ? options.hostname : null;

  this.setNoDelay();
  this.setKeepAlive(true);
};

exports.Connection = Connection;
inherits(Connection, Stream);

Connection.prototype.connect = function(port, host) {
  var packet;
  var datacache = "";
  var hostname;

  Stream.prototype.connect.call(this, port, host);

  hostname = this._hostname || (host && host.toString()) || "localhost";

  packet = new Buffer(HANDSHAKE_HEADER.length + hostname.length + 1);

  packet.write(HANDSHAKE_HEADER, "ascii");
  packet[HANDSHAKE_CODE_OFF] = hostname.length;
  packet.write(host, HANDSHAKE_CODE_OFF + 1, "ascii");
  this.write(packet);

  this.ondata = function(data, start, end) {

    datacache += data.toString("binary", start, end);

    if (datacache.length < HANDSHAKE_HEADER.length + 1) {
      return;
    } else if (datacache.length > HANDSHAKE_SIZE) {
      this.destroy(new Error("Bad handshake response packet."));
    } else {
      var code = datacache.charCodeAt(HANDSHAKE_CODE_OFF);
      if (code != 0) {
        this.destroy(new Error("Handshake error #" + code));
      } else {
        this.ondata = packetParser;
        this.emit("handshake");
      }
    }
  };
};

Connection.prototype.writeOpen = function(ch, mode, data, callback) {

  if (!data) {
    data = new Buffer("");
  }

  if (!Buffer.isBuffer(data)) {
    data = new Buffer(data, "utf8");
  }

  if (callback) {
    if (this._openCallbacks[ch]) {
      this._openCallbacks[ch].push(callback);
    } else {
      this._openCallbacks[ch] = [callback];
    }
  }

  mode = getBinMode(mode);

  if (typeof mode !== "number") {
    throw new Error("Invalid mode");
  }

  return this._writePacket(ch, OPEN << 4 | mode, data);
};

Connection.prototype.writeData = function(ch, priority, data, callback) {

  if (!data) {
    data = new Buffer("");
  }

  if (!Buffer.isBuffer(data)) {
    data = new Buffer(data, "utf8");
  }

  return this._writePacket(ch, DATA << 4 | priority, data, callback);
};

Connection.prototype.writeSignal = function(ch, type, data, callback) {

  if (!data) {
    data = new Buffer("");
  }

  if (!Buffer.isBuffer(data)) {
    data = new Buffer(data, "utf8");
  }

  return this._writePacket(ch, SIGNAL << 4 | type, data, callback);
};


Connection.prototype._writePacket = function(ch, flag, data, callback) {
  var length = 8 + data.length;
  var packet = new Buffer(length);

  packet[0] = length >>> 8;
  packet[1] = length % 256;
  packet[2] = 0;
  packet[3] = ch >>> 24;
  packet[4] = ch >>> 16;
  packet[5] = ch >>> 8;
  packet[6] = ch % 256;
  packet[7] = flag;

  try {

    if (data.length) {
      data.copy(packet, 8, 0);
    }

    return this.write(packet, callback);
  } catch (writeException) {
    this.destroy(writeException);
    return false;
    return;
  }

  return true;
};

function packetParser(data, start, end) {
  var readbuffer = this._readbuffer;
  var readpos = this._readpos;
  var readend = this._readend;
  var target;
  var ch;
  var response;
  var flag;
  var payload;
  var eventname;
  var bufferlength;
  var packetlength;
  var exception;

  if (readbuffer) {
    readbuffer = combindBuffers(readbuffer, readpos, readend, data, start, end);
    bufferlength = readbuffer.length;
    readpos = 0;
  } else {
    readbuffer = data;
    readpos = start;
    bufferlength = end;
  }

  while (readpos < bufferlength) {

    if (readpos + 0x02 > bufferlength) {
      break;
    }

    packetlength = (readbuffer[readpos] * 256) + readbuffer[readpos + 1];

    if (readpos + packetlength > bufferlength) {
      break;
    }

    ch = (readbuffer[readpos + 0x03] * 256 * 256 * 256) +
         (readbuffer[readpos + 0x04] * 256 * 256) +
         (readbuffer[readpos + 0x05] * 256) +
         (readbuffer[readpos + 0x06]);

    flag = (readbuffer[readpos + 0x07] & 0xf);
    payload = readbuffer.slice( readpos + 0x08
                              , readpos + packetlength);

    switch (readbuffer[readpos + 0x07] >> 4) {

      case OPEN:
        if (this._openCallbacks[ch]) {
          var cbs = this._openCallbacks[ch];
          this._openCallbacks[ch] = null;
          for (var i = 0; i < cbs.length; i++) {
            cbs[i].call(this, ch, flag, payload);
          }
        } else {
          if (flag > 2 && this._throwErrors) {
            var msg = "Open Error #" + flag + " " + payload.toString("utf8");
            this.emit("error", new Error(msg));
          } else {
            this.emit("open", ch, flag, payload);
          }
        }
        break;

      case DATA:
        this.emit("message", ch, flag, payload);
        break;

      case SIGNAL:
        if (flag > 1 && this._throwErrors) {
          var msg = "Signal Error #" + flag + " " + payload.toString("utf8");
          this.emit("error", new Error(msg));
        } else {
          this.emit("signal", ch, flag, payload);
        }
        break;

      default:
        this.destroy(new Error("Server sent bad operator " + (readbuffer[readpos + 0x07] >> 4)));
        return;
    }

    readpos += packetlength;
  }

  if (bufferlength - readpos == 0) {
    this._readbuffer = null;
  } else {
    this._readbuffer = readbuffer;
    this._readpos = readpos;
    this._readend = bufferlength;
  }
}

function combindBuffers(buffera, starta, enda, bufferb, startb, endb) {
  var length = (enda - starta) + (endb - startb);
  var newbuffer = new Buffer(length);
  buffera.copy(newbuffer, 0, starta, enda);
  bufferb.copy(newbuffer, (enda - starta), startb, endb);
  return newbuffer;
}

function getBinMode(modeExpr) {
  var result = 0;
  var match;

  if (!modeExpr) {
    return 0;
  }

  if (typeof modeExpr !== "string" || !(match = modeExpr.match(MODE_RE))) {
    return null;
  }

  match[1] && (result |= READ);
  match[2] && (result |= WRITE);
  match[3] && (result |= EMIT);

  return result;
}
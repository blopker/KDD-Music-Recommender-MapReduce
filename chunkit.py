#! /usr/bin/env python
import sys

if len(sys.argv) != 4:
    print "Usage: chunkit.py database_file songs_per_chunk chunk_name_prefix"

songsPerChunk = int(sys.argv[2])
chunckName = sys.argv[3]


class Song():
    def __init__(self, line):
        line = line.split("\t")
        if len(line) < 2:
            print "error, bad song, terminate. ", line
            sys.exit(1)
        self.id = int(line[0])
        self.rating = line[1]

    def toString(self):
        return str(self.id) + "\t" + self.rating

    def getChunkName(self):
        return chunckName + str(self.id / songsPerChunk) + ".txt"


class User():

    def __init__(self, line):
        self.songList = []
        line = line.split("|")
        if len(line) < 2:
            print "error, bad user, terminate. ", line
            sys.exit(1)
        self.id = line[0]
        self.songCount = int(line[1])
        self.totalRating = 0

    def addSong(self, line):
        song = Song(line)
        self.songList.append(song)
        self.totalRating += int(song.rating)

    def toString(self):
        return self.id + "|" + str(self.songCount) + "|" + str(self.totalRating/float(self.songCount))


class Buffer():
    def __init__(self):
        self.buff = {}
        self.count = 0
        self.countTotal = 0
        self.currentUser = False

    def add(self, user):
        if self.count > 3000:
            self.write()
        if self.currentUser:
            self.chunk()
        self.currentUser = User(user)
        self.count += 1

    def addSong(self, song):
        self.currentUser.addSong(song)

    def chunk(self):
        s = set()
        li = self.currentUser.songList

        for song in li:
            s.add(song.getChunkName())

        for chunk in s:
            if not chunk in self.buff:
                self.buff[chunk] = []
            self.buff[chunk].append(self.currentUser.toString())

        for song in li:
            self.buff[song.getChunkName()].append(song.toString())

        # print "Buffered user: " + self.currentUser.toString()
        self.currentUser = False

    def write(self):
        self.chunk()
        for chunk, li in self.buff.iteritems():
            with open(chunk, "w+") as f:
                for line in li:
                    f.write(line + "\n")
        self.countTotal += self.count
        print "Wrote " + str(self.countTotal) + " users."
        self.count = 0
        self.buff = {}

with open(sys.argv[1]) as f:
    buff = Buffer()
    for line in f:
        if line.find("|") > -1:
            buff.add(line)
        else:
            buff.addSong(line)
    buff.write()

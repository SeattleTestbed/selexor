'''
<Program>
  formatter.py

<Purpose>
  Formats data obtained from [] and [] to work with SeleXor.

<Started>
  06.23.2012

<Author>
  Leonard Law
  leon.wlaw@gmail.com


'''
import sys

DATATYPE = sys.argv[1]

def skiplines(input):
  skip = 0
  if DATATYPE == 'country':
    skip = 2
  elif DATATYPE == 'city':
    skip = 1
  for line in range(skip):
    input.readline()


def writetokens(tokens, output):
  if DATATYPE == 'country':
    data = tokens[1] + "\t" + tokens[0] + "\n"
  elif DATATYPE == 'city':
    data = tokens[2] + "\t" + tokens[1] + "\n"
  output.write(data)


def parseline(line, input):
  if DATATYPE == 'country':
    splitchar = '\t'
  elif DATATYPE == 'city':
    splitchar = ','
  line_tokens = line.split(splitchar)
  if DATATYPE == 'country':
    # Some lines are terminated prematurely
    while len(line_tokens) < 14:
      new_tokens = input.readline().split('\t')
      line_tokens[-1] = line_tokens[-1][:-1] # Get rid of \n at the end
      line_tokens[-1] = line_tokens[-1] + " " + new_tokens[0]
      new_tokens = new_tokens[1:] # Get rid of the token that was just used
      line_tokens += new_tokens
  return line_tokens


def main():
  input = open(DATATYPE + "_src.txt", 'r')
  output = open(DATATYPE + ".txt", 'w')
  skiplines(input)
  line_count = 0
  line = input.readline()
  while line:
    line_tokens = parseline(line, input)
    writetokens(line_tokens, output)
    line = input.readline()
    line_count += 1
  print "Read a total of", line_count, "lines"
  input.close()
  output.close()

if __name__ == '__main__':
  main()

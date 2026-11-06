- Reading one branch's ref takes one storage request instead of three, so a
  write, merge or delete on a named branch sends 6 to 10 fewer requests.

- Embedded hosts can pass `OptimizeRequest::with_before_publish`: it runs once
  before an optimize run publishes, with the commit's nonce and input head, and
  its failure publishes nothing.

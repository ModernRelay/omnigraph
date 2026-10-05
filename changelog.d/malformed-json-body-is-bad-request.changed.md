- A malformed JSON request body is refused with the JSON `ErrorOutput` body
  under the code `bad_request`; no `json_rejection` code exists. The HTTP status
  is 400 for a syntax or shape failure, 413 for a body over the limit and 415
  for a missing or wrong JSON content type.

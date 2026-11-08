- A null assigned to a nullable Blob clears it. `update … set { content:
  $content }` with `$content: Blob?` bound to null now writes a null cell.
  Earlier updates kept the old value and still published a commit.

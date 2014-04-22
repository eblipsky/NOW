function(doc) {
  if(doc.doc_type == 'Log') {
    emit([doc.fq, doc.start], doc);
  }
}

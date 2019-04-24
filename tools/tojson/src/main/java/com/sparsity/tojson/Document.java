package com.sparsity.tojson;

/**
 * Created by aprat on 28/03/17.
 */
public class Document {

    public enum Type {
        like,
        post,
        comment
    }

    private long idDoc = -1;
    private long date = -1;
    private Document parent = null;
    private Type type = null;
    private String [] tags = null;
    private Person person = null;
    private String country = null;

    public Document(long idDoc, long date, Document parent, Type type, String[] tags, Person person, String country) {
        this.idDoc = idDoc;
        this.date = date;
        this.parent = parent;
        this.type = type;
        this.tags = tags;
        this.person = person;
        this.country = country;
    }
}

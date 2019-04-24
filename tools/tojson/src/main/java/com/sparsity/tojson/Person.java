package com.sparsity.tojson;

/**
 * Created by aprat on 28/03/17.
 */
public class Person {
    public long idPerson = -1;
    public String country = null;
    public String city = null;
    public String firstName = null;
    public String lastName = null;

    public Person(long idPerson, String country, String city, String firstName, String lastName) {
        this.idPerson = idPerson;
        this.country = country;
        this.city = city;
        this.firstName = firstName;
        this.lastName = lastName;
    }
}

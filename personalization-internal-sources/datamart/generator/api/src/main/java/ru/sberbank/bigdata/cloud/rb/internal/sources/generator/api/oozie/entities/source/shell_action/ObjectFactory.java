
package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.source.shell_action;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the ru.sberbank.bigdata.cloud.rb.internal.sources.common.oozie.entities.source.shell_action package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _Shell_QNAME = new QName("uri:oozie:shell-action:0.3", "shell");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: ru.sberbank.bigdata.cloud.rb.internal.sources.common.oozie.entities.source.shell_action
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link CONFIGURATION }
     * 
     */
    public CONFIGURATION createCONFIGURATION() {
        return new CONFIGURATION();
    }

    /**
     * Create an instance of {@link ACTION }
     * 
     */
    public ACTION createACTION() {
        return new ACTION();
    }

    /**
     * Create an instance of {@link DELETE }
     * 
     */
    public DELETE createDELETE() {
        return new DELETE();
    }

    /**
     * Create an instance of {@link PREPARE }
     * 
     */
    public PREPARE createPREPARE() {
        return new PREPARE();
    }

    /**
     * Create an instance of {@link FLAG }
     * 
     */
    public FLAG createFLAG() {
        return new FLAG();
    }

    /**
     * Create an instance of {@link MKDIR }
     * 
     */
    public MKDIR createMKDIR() {
        return new MKDIR();
    }

    /**
     * Create an instance of {@link CONFIGURATION.Property }
     * 
     */
    public CONFIGURATION.Property createCONFIGURATIONProperty() {
        return new CONFIGURATION.Property();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ACTION }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "uri:oozie:shell-action:0.3", name = "shell")
    public JAXBElement<ACTION> createShell(ACTION value) {
        return new JAXBElement<ACTION>(_Shell_QNAME, ACTION.class, null, value);
    }

}

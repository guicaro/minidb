// Automatically generated code.  Edit at your own risk!
// Generated by bali2jak v2002.09.03.

package mdb;
import Jakarta.util.*;
import java.io.*;
import java.util.*;

public class Plist extends Proj_list {

    final public static int ARG_LENGTH = 1 ;
    final public static int TOK_LENGTH = 1 /* Kludge! */ ;

    public void execute () {
        
        super.execute();
    }

    public Spec_list getSpec_list () {
        
        return (Spec_list) arg [0] ;
    }

    public boolean[] printorder () {
        
        return new boolean[] {false} ;
    }

    public Plist setParms (Spec_list arg0) {
        
        arg = new AstNode [ARG_LENGTH] ;
        tok = new AstTokenInterface [TOK_LENGTH] ;
        
        arg [0] = arg0 ;            /* Spec_list */
        
        InitChildren () ;
        return (Plist) this ;
    }

}

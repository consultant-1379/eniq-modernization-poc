/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2012
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.kyo.workers.spark;

import java.sql.Types;

import org.apache.spark.sql.jdbc.*;
import org.apache.spark.sql.types.*;

import scala.Option;

public class VerticaDialect extends JdbcDialect {

    @Override
    public boolean canHandle(String arg0) {
        return arg0!=null && arg0.startsWith("jdbc:vertica");
    }
    
    @Override
    public Option<JdbcType> getJDBCType(final DataType dt) {
        if(dt == DataTypes.StringType) {
            return Option.apply(JdbcType.apply("VARCHAR(255)", Types.VARCHAR));
        }
        return PostgresDialect.getJDBCType(dt);
    }
    
    @Override
    public Option<DataType> getCatalystType(final int sqlType, final String typeName, final int size, final MetadataBuilder md) {
        return PostgresDialect.getCatalystType(sqlType, typeName, size, md);
    }
}
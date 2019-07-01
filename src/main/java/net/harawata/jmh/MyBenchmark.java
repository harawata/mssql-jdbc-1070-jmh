/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package net.harawata.jmh;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
public class MyBenchmark {
  private static final String url = "jdbc:sqlserver://127.0.0.1:1433;databaseName=test";
  private static final String user = "sa";
  private static final String password = "Abcdefg7";
  private static final String table = "test";

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }

  @Benchmark
  public void testMethod(Blackhole blackhole) {
    try (Connection con = getConnection();
        PreparedStatement ps = con.prepareStatement("select d from " + table);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        blackhole.consume(rs.getObject("d", LocalDate.class));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Setup
  public void setup() throws Exception {
    try (Connection con = getConnection();
        Statement stmt = con.createStatement()) {

      DatabaseMetaData dbmd = con.getMetaData();
      System.out.println(">>> DB version : " + dbmd.getDatabaseProductName() + " " + dbmd.getDatabaseProductVersion());
      System.out.println(">>> Driver version : " + dbmd.getDriverVersion());

      try {
        stmt.execute("drop table " + table);
      } catch (SQLException e) {
        // table did not exist
      }
      // poppulate table with random dates
      stmt.execute("create table " + table + " (d date)");
      stmt.execute("insert into " + table + " select dateadd(day, -rand(12313456) * 1000, getdate())");
      stmt.execute(
          "while((select count(*) from " + table + ") < 2500) insert into " + table
              + " select dateadd(day, -rand() * 1000, getdate())");
    }
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }
}

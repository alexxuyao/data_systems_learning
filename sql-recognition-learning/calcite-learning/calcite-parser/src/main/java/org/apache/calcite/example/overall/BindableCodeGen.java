package org.apache.calcite.example.overall;

import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FieldDeclaration;
import org.apache.calcite.linq4j.tree.VisitorImpl;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.runtime.Utilities;
import org.codehaus.commons.compiler.*;
import org.codehaus.commons.compiler.util.ResourceFinderClassLoader;
import org.codehaus.commons.compiler.util.resource.*;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class BindableCodeGen {

    public static Bindable getBindable(BindableCodeResult codeResult)
            throws CompileException, IOException, ExecutionException {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
        }
        final IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName(codeResult.getClassName());
        cbe.setExtendedClass(Utilities.class);
        cbe.setImplementedInterfaces(
                codeResult.getFieldCount() == 1
                        ? new Class[]{Bindable.class, Typed.class}
                        : new Class[]{ArrayBindable.class});
        cbe.setParentClassLoader(EnumerableInterpretable.class.getClassLoader());
        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation(true, true, true);
        }

        return (Bindable) cbe.createInstance(new StringReader(codeResult.getBodyCode()));
    }

    public static Bindable getBindable2(BindableCodeResult codeResult) throws CompileException, IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
        }

        ICompiler compiler = compilerFactory.newCompiler();

        // Store generated .class files in a Map:
        Map<String, byte[]> classes = new HashMap<String, byte[]>();
        ResourceCreator resourceCreator = new MapResourceCreator(classes);
        compiler.setClassFileCreator(resourceCreator);

        // Now compile two units from strings:
        compiler.compile(new  Resource[]{
                new StringResource(
                        codeResult.getClassName().replace(".", "/") + ".java",
                        BindableCodeResult.toClassCode(codeResult)
                )
        });

        // Set up a class loader that uses the generated classes.
        ClassLoader cl = new ResourceFinderClassLoader(
                new MapResourceFinder(classes),    // resourceFinder
                ClassLoader.getSystemClassLoader() // parent
        );

        return (Bindable) cl.loadClass(codeResult.getClassName()).getConstructor().newInstance();
        // Assert.assertEquals(77, cl.loadClass("pkg1.A").getDeclaredMethod("meth").invoke(null));
    }


    public static BindableCodeResult toBindableCode(Map<String, Object> parameters,
                                                    EnumerableRel rel,
                                                    EnumerableRel.Prefer prefer) {
        EnumerableRelImplementor relImplementor = new EnumerableRelImplementor(rel.getCluster().getRexBuilder(),
                parameters);

        final ClassDeclaration expr = relImplementor.implementRoot(rel, prefer);
        String classBodyCode = Expressions.toString(expr.memberDeclarations, "\n", false);

//        if (CalciteSystemProperty.DEBUG.value()) {
//            Util.debugCode(System.out, classBodyCode);
//        }

        // Hook.JAVA_PLAN.run(classBodyCode);
        StaticFieldDetector detector = new StaticFieldDetector();
        expr.accept(detector);
        Boolean containsStaticField = detector.containsStaticField;

        String className = UUID.randomUUID().toString().replaceAll("-", "");

        return new BindableCodeResult("cn.bdes.Baz" + className, containsStaticField, classBodyCode, rel.getRowType().getFieldCount());
    }

    static class StaticFieldDetector extends VisitorImpl<Void> {
        boolean containsStaticField = false;

        @Override
        public Void visit(final FieldDeclaration fieldDeclaration) {
            containsStaticField = (fieldDeclaration.modifier & Modifier.STATIC) != 0;
            return containsStaticField ? null : super.visit(fieldDeclaration);
        }
    }

    public static class BindableCodeResult {
        private String className;
        private Boolean containsStaticField;
        private String bodyCode;
        private int fieldCount;

        public BindableCodeResult(String className, Boolean containsStaticField, String bodyCode, int fieldCount) {
            this.className = className;
            this.containsStaticField = containsStaticField;
            this.bodyCode = bodyCode;
            this.fieldCount = fieldCount;
        }

        public BindableCodeResult() {

        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public Boolean getContainsStaticField() {
            return containsStaticField;
        }

        public void setContainsStaticField(Boolean containsStaticField) {
            this.containsStaticField = containsStaticField;
        }

        public String getBodyCode() {
            return bodyCode;
        }

        public void setBodyCode(String bodyCode) {
            this.bodyCode = bodyCode;
        }

        public int getFieldCount() {
            return fieldCount;
        }

        public void setFieldCount(int fieldCount) {
            this.fieldCount = fieldCount;
        }

        public static String toClassCode(BindableCodeResult codeResult) {

            String className = codeResult.getClassName();

            String packageName = className.substring(0, className.lastIndexOf("."));
            String shortClassName = className.substring(className.lastIndexOf(".") + 1);
            StringBuilder sb = new StringBuilder();
            sb.append("package ").append(packageName).append(";\n");
            sb.append("import org.apache.calcite.runtime.*;\n");
            sb.append("public class ").append(shortClassName).append(" extends Utilities implements ");

            if (codeResult.getFieldCount() == 1) {
                sb.append("Bindable, Typed {\n");
            } else {
                sb.append("ArrayBindable {\n");
            }

            sb.append(codeResult.getBodyCode());
            sb.append("\n}");

            return sb.toString();
        }
    }
}

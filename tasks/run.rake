


module Buildr
  module Run

    class JavaRunner < Base
      include Buildr::JRebel

      def run(task)
        fail "Missing :main option" unless task.options[:main]

        # create classpath with our app paths first, then jars
        paths = project.compile.dependencies + [project.path_to(:target, :classes)] + task.classpath
        cp = []
        paths.each do |p|
          cp << p.to_s
        end
        cp = cp.sort.uniq.reverse

        args = " "
        if ENV["args"] then
          args += ENV["args"]
        end
        cmd = task.options[:main] + args
        full_cmd = "java -classpath " + cp.join(":") + " " + cmd

        #puts cp

        if task.options[:verbose]
          puts
          puts full_cmd
          puts
        end

        puts
        puts
        puts "launching app: " + cmd
        puts
        puts
        system(full_cmd)
        return

        # doesn't properly handle our app args :/
        #Java::Commands.java(task.options[:main], {
        #  :properties => jrebel_props(project).merge(task.options[:properties] || {}),
        #  :classpath => cp,
        #  :java_args => jrebel_args + (task.options[:java_args] || [])
        #})
      end
    end # JavaRunnner

  end
end


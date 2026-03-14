class Androidmigrate < Formula
  include Language::Python::Virtualenv

  desc "Checkpointed Android folder backup and sync over ADB"
  homepage "https://github.com/MachineLearning-Nerd/AndroidMigrate"
  url "https://github.com/MachineLearning-Nerd/AndroidMigrate/archive/refs/tags/v0.1.3.tar.gz"
  sha256 "PLACEHOLDER"
  license "MIT"

  depends_on "python@3.12"

  def install
    virtualenv_install_with_resources
  end

  def caveats
    <<~EOS
      androidmigrate requires adb (Android Debug Bridge) to communicate with devices.
      Install it via:
        brew install --cask android-platform-tools
    EOS
  end

  test do
    assert_match "usage:", shell_output("#{bin}/androidmigrate --help", 0)
  end
end

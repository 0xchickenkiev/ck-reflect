USE [master]
GO

CREATE DATABASE [Lake_Control]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'Lake_Control', FILENAME = N'J:\Data\LAKE\Lake_Control\Lake_Control.mdf' , SIZE = 160041344KB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB )
 LOG ON 
( NAME = N'Lake_Control_Log', FILENAME = N'G:\Logs\LAKE\Lake_Control\Lake_Control.ldf' , SIZE = 5242880KB , MAXSIZE = 2048GB , FILEGROWTH = 1048576KB )
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [Lake_Control].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO

ALTER DATABASE [Lake_Control] SET ANSI_NULL_DEFAULT ON 
GO

ALTER DATABASE [Lake_Control] SET ANSI_NULLS ON 
GO

ALTER DATABASE [Lake_Control] SET ANSI_PADDING OFF 
GO

ALTER DATABASE [Lake_Control] SET ANSI_WARNINGS OFF 
GO

ALTER DATABASE [Lake_Control] SET ARITHABORT OFF 
GO

ALTER DATABASE [Lake_Control] SET AUTO_CLOSE OFF 
GO

ALTER DATABASE [Lake_Control] SET AUTO_SHRINK OFF 
GO

ALTER DATABASE [Lake_Control] SET AUTO_UPDATE_STATISTICS ON 
GO

ALTER DATABASE [Lake_Control] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO

ALTER DATABASE [Lake_Control] SET CURSOR_DEFAULT  GLOBAL 
GO

ALTER DATABASE [Lake_Control] SET CONCAT_NULL_YIELDS_NULL OFF 
GO

ALTER DATABASE [Lake_Control] SET NUMERIC_ROUNDABORT OFF 
GO

ALTER DATABASE [Lake_Control] SET QUOTED_IDENTIFIER ON 
GO

ALTER DATABASE [Lake_Control] SET RECURSIVE_TRIGGERS OFF 
GO

ALTER DATABASE [Lake_Control] SET  DISABLE_BROKER 
GO

ALTER DATABASE [Lake_Control] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO

ALTER DATABASE [Lake_Control] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO

ALTER DATABASE [Lake_Control] SET TRUSTWORTHY OFF 
GO

ALTER DATABASE [Lake_Control] SET ALLOW_SNAPSHOT_ISOLATION ON 
GO

ALTER DATABASE [Lake_Control] SET PARAMETERIZATION SIMPLE 
GO

ALTER DATABASE [Lake_Control] SET READ_COMMITTED_SNAPSHOT OFF 
GO

ALTER DATABASE [Lake_Control] SET HONOR_BROKER_PRIORITY OFF 
GO

ALTER DATABASE [Lake_Control] SET RECOVERY FULL 
GO

ALTER DATABASE [Lake_Control] SET  MULTI_USER 
GO

ALTER DATABASE [Lake_Control] SET PAGE_VERIFY CHECKSUM  
GO

ALTER DATABASE [Lake_Control] SET DB_CHAINING OFF 
GO

ALTER DATABASE [Lake_Control] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO

ALTER DATABASE [Lake_Control] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO

ALTER DATABASE [Lake_Control] SET DELAYED_DURABILITY = DISABLED 
GO

ALTER DATABASE [Lake_Control] SET QUERY_STORE = OFF
GO

USE [Lake_Control]
GO

ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO

ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO

ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO

ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO

ALTER DATABASE [Lake_Control] SET  READ_WRITE 
GO


